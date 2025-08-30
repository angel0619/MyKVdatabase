#include "scheduler.h"

static bool debug = false;

namespace sylar {
// 当前协程调度器,局部调度器
static thread_local Scheduler* t_scheduler = nullptr;

Scheduler* Scheduler::GetThis()
{
	return t_scheduler;
}
// 把 t_scheduler 设为 this，把当前线程和该 Scheduler 绑定起来
void Scheduler::SetThis()
{
	t_scheduler = this;
}
/*
	threads:总工作线程数量。
	use_caller:是否让主线程也参与任务执行。
	name:调度器名称。
*/
Scheduler::Scheduler(size_t threads, bool use_caller, const std::string &name):
m_useCaller(use_caller), m_name(name)	// 成员初始化列表
{
	// 要有一个工作线程 && 每个线程只能挂一个 Scheduler
	assert(threads>0 && Scheduler::GetThis()==nullptr);

	SetThis();

	Thread::SetName(m_name); // 给当前 OS 线程设置名字

	// 使用主线程当作工作线程，把当前线程（通常是 main 线程）也纳入调度
	if(use_caller)
	{
		// 主线程也参与任务执行，主线程占一个“工作名额”
		threads --;

		// 创建主协程
		Fiber::GetThis();

		// 创建调度协程
		// Scheduler::run 入口函数，调度主循环。
		// false -> 该调度协程退出后将返回主协程
		m_schedulerFiber.reset(new Fiber(std::bind(&Scheduler::run, this), 0, false)); 
		
		// 本线程的调度协程指针记录到 TLS（线程本地存储）
		Fiber::SetSchedulerFiber(m_schedulerFiber.get());
		// 记录线程
		m_rootThread = Thread::GetThreadId();
		m_threadIds.push_back(m_rootThread);
	}
	// 除调用方线程之外，需要的工作线程
	m_threadCount = threads;
	if(debug) std::cout << "Scheduler::Scheduler() success\n";
}

Scheduler::~Scheduler()
{
	assert(stopping()==true);
	if (GetThis() == this) 
	{
        t_scheduler = nullptr;
    }
    if(debug) std::cout << "Scheduler::~Scheduler() success\n";
}

// 启动调度器的工作线程
void Scheduler::start()
{
	// 保证线程安全
	std::lock_guard<std::mutex> lock(m_mutex);
	// 如果已经停止
	if(m_stopping)
	{
		std::cerr << "Scheduler is stopped" << std::endl;
		return;
	}
	// 确保没重复启动
	assert(m_threads.empty());
	// 减去use_caller 的工作线程的数量m_threadCount
	m_threads.resize(m_threadCount);
	for(size_t i=0; i<m_threadCount; i++)
	{
		// 每个线程启动时，都会执行调度主循环
		m_threads[i].reset(new Thread(std::bind(&Scheduler::run, this), m_name + "_" + std::to_string(i)));
		m_threadIds.push_back(m_threads[i]->getId()); //记录线程ID
	}
	if(debug) std::cout << "Scheduler::start() success\n";
}

// 调度器的核心逻辑
void Scheduler::run()
{
	int thread_id = Thread::GetThreadId();
	if(debug) std::cout << "Schedule::run() starts in thread: " << thread_id << std::endl;
	
	//set_hook_enable(true);
	// 把当前线程绑定到这个调度器
	SetThis();

	// 运行在新创建的线程 -> 需要创建主协程
	if(thread_id != m_rootThread)
	{
		Fiber::GetThis();
	}

	std::shared_ptr<Fiber> idle_fiber = std::make_shared<Fiber>(std::bind(&Scheduler::idle, this));
	ScheduleTask task;
	
	while(true)
	{
		task.reset();
		bool tickle_me = false;

		{
			std::lock_guard<std::mutex> lock(m_mutex);
			auto it = m_tasks.begin();
			// 1 遍历任务队列
			while(it!=m_tasks.end())
			{
				if(it->thread!=-1&&it->thread!=thread_id)
				{
					it++;
					tickle_me = true;	// 还有其它任务留给其他线程
					continue;
				}

				// 2 取出任务
				assert(it->fiber||it->cb);
				task = *it;
				m_tasks.erase(it); 
				m_activeThreadCount++;
				break;
			}	
			tickle_me = tickle_me || (it != m_tasks.end());
		}
		// 通知其它线程有任务
		if(tickle_me)
		{
			tickle();
		}

		// 3 执行任务
		if(task.fiber)
		{
			{					
				std::lock_guard<std::mutex> lock(task.fiber->m_mutex);
				if(task.fiber->getState()!=Fiber::TERM)
				{
					task.fiber->resume();	
				}
			}
			m_activeThreadCount--;
			task.reset();
		}
		else if(task.cb)
		{
			std::shared_ptr<Fiber> cb_fiber = std::make_shared<Fiber>(task.cb);
			{
				std::lock_guard<std::mutex> lock(cb_fiber->m_mutex);
				cb_fiber->resume();			
			}
			m_activeThreadCount--;
			task.reset();	
		}
		// 4 无任务 -> 执行空闲协程
		else
		{		
			// 系统关闭 -> idle协程将从死循环跳出并结束 -> 此时的idle协程状态为TERM -> 再次进入将跳出循环并退出run()
            if (idle_fiber->getState() == Fiber::TERM) 
            {
            	if(debug) std::cout << "Schedule::run() ends in thread: " << thread_id << std::endl;
                break;
            }
			// 否则执行 idle_fiber->resume()，等它 yield() 回来。
			m_idleThreadCount++;
			idle_fiber->resume();				
			m_idleThreadCount--;
		}
	}
	
}

void Scheduler::stop()
{
	if(debug) std::cout << "Schedule::stop() starts in thread: " << Thread::GetThreadId() << std::endl;
	
	if(stopping())
	{
		return;
	}

	m_stopping = true;	

    if (m_useCaller) 
    {
        assert(GetThis() == this);
    } 
    else 
    {
        assert(GetThis() != this);
    }
	
	for (size_t i = 0; i < m_threadCount; i++) 
	{
		tickle();
	}

	if (m_schedulerFiber) 
	{
		tickle();
	}

	if(m_schedulerFiber)
	{
		m_schedulerFiber->resume();
		if(debug) std::cout << "m_schedulerFiber ends in thread:" << Thread::GetThreadId() << std::endl;
	}

	std::vector<std::shared_ptr<Thread>> thrs;
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		thrs.swap(m_threads);
	}

	for(auto &i : thrs)
	{
		i->join();
	}
	if(debug) std::cout << "Schedule::stop() ends in thread:" << Thread::GetThreadId() << std::endl;
}

void Scheduler::tickle()
{
}

void Scheduler::idle()
{
	while(!stopping())
	{
		if(debug) std::cout << "Scheduler::idle(), sleeping in thread: " << Thread::GetThreadId() << std::endl;	
		sleep(1);	
		Fiber::GetThis()->yield();
	}
}

bool Scheduler::stopping() 
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_stopping && m_tasks.empty() && m_activeThreadCount == 0;
}


}