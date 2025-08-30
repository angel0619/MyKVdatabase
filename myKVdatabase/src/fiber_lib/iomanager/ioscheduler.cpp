#include <unistd.h>    
#include <sys/epoll.h> 
#include <fcntl.h>     
#include <cstring>

#include "ioscheduler.h"

static bool debug = true;

namespace sylar {
// 获取当前线程绑定的 IOManager 对象。
IOManager* IOManager::GetThis() 
{
    return dynamic_cast<IOManager*>(Scheduler::GetThis());
}
// 根据事件类型（读/写）返回对应的 EventContext
IOManager::FdContext::EventContext& IOManager::FdContext::getEventContext(Event event) 
{
    assert(event==READ || event==WRITE);    
    switch (event) 
    {
    case READ:
        return read;
    case WRITE:
        return write;
    }
    throw std::invalid_argument("Unsupported event type");
}
// 事件触发后，清空原来的绑定信息，防止重复触发或悬空指针
void IOManager::FdContext::resetEventContext(EventContext &ctx) 
{
    ctx.scheduler = nullptr;
    ctx.fiber.reset();
    ctx.cb = nullptr;
}

// no lock
void IOManager::FdContext::triggerEvent(IOManager::Event event) {
    assert(events & event);

    // delete event 
    events = (Event)(events & ~event);
    
    // trigger，获取事件上下文
    EventContext& ctx = getEventContext(event);
    if (ctx.cb) 
    {
        // 调度回调函数
        // call ScheduleTask(std::function<void()>* f, int thr)
        ctx.scheduler->scheduleLock(&ctx.cb);
    } 
    else 
    {
        // 调度协程
        // call ScheduleTask(std::shared_ptr<Fiber>* f, int thr)
        ctx.scheduler->scheduleLock(&ctx.fiber);
    }

    // reset event context
    resetEventContext(ctx);
    return;
}
// 构造函数
IOManager::IOManager(size_t threads, bool use_caller, const std::string &name): 
Scheduler(threads, use_caller, name), TimerManager()
{
    // create epoll fd创建 epoll fd
    m_epfd = epoll_create(5000);
    assert(m_epfd > 0);

    // create pipe创建通知管道
    int rt = pipe(m_tickleFds);
    assert(!rt);

    // add read event to epoll，将管道读端加入 epoll
    epoll_event event;
    event.events  = EPOLLIN | EPOLLET; // Edge Triggered
    event.data.fd = m_tickleFds[0];

    // non-blocked
    rt = fcntl(m_tickleFds[0], F_SETFL, O_NONBLOCK);
    assert(!rt);
    // //将所有需要监听的socket添加到epfd中
    rt = epoll_ctl(m_epfd, EPOLL_CTL_ADD, m_tickleFds[0], &event);
    assert(!rt);
    // 初始化 fd 上下文（每个 fd 一个 FdContext）
    contextResize(32);

    start();
}

IOManager::~IOManager() {
    stop();
    close(m_epfd);
    close(m_tickleFds[0]);
    close(m_tickleFds[1]);

    for (size_t i = 0; i < m_fdContexts.size(); ++i) 
    {
        if (m_fdContexts[i]) 
        {
            delete m_fdContexts[i];
        }
    }
}

// no lock
// 确保每个文件描述符（fd）都有对应的上下文对象 FdContext，调整FD上下文的大小
void IOManager::contextResize(size_t size) 
{
    // m_fdContexts：所有被监听的文件描述符（fd）上下文的数组/容器
    m_fdContexts.resize(size);

    for (size_t i = 0; i < m_fdContexts.size(); ++i) 
    {
        if (m_fdContexts[i]==nullptr) 
        {
            // 如果某个位置为空 (nullptr)，就 new 一个 FdContext
            m_fdContexts[i] = new FdContext();
            m_fdContexts[i]->fd = i;
        }
    }
}

int IOManager::addEvent(int fd, Event event, std::function<void()> cb) 
{
    // attemp to find FdContext 获取 fd 上下文
    FdContext *fd_ctx = nullptr;
    // 读锁
    std::shared_lock<std::shared_mutex> read_lock(m_mutex);
    if ((int)m_fdContexts.size() > fd) // fd小于容器大小，可以直接使用，上下文已存在
    {
        fd_ctx = m_fdContexts[fd];
        read_lock.unlock();
    }
    else 
    {
        // 如果 fd 超出当前容器大小，则调用 contextResize 扩容。
        read_lock.unlock();
        std::unique_lock<std::shared_mutex> write_lock(m_mutex);
        // 会扩展容器并为新增位置创建新的 FdContext
        contextResize(fd * 1.5);
        // 从 m_fdContexts 容器中获取对应文件描述符 fd 的上下文对象
        fd_ctx = m_fdContexts[fd];
    }

    // 加锁 fd 上下文，防止多个线程同时修改同一个 fd 的事件和协程上下文。
    std::lock_guard<std::mutex> lock(fd_ctx->mutex);
    
    // the event has already been added
    if(fd_ctx->events & event) 
    {
        return -1;
    }

    // add new event
    // EPOLL_CTL_MOD：如果之前有事件，用来修改
    int op = fd_ctx->events ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
    epoll_event epevent;
    // 边缘触发
    epevent.events   = EPOLLET | fd_ctx->events | event;
    epevent.data.ptr = fd_ctx;

    int rt = epoll_ctl(m_epfd, op, fd, &epevent);
    if (rt) 
    {
        std::cerr << "addEvent::epoll_ctl failed: " << strerror(errno) << std::endl; 
        return -1;
    }

    ++m_pendingEventCount;

    // update fdcontext
    fd_ctx->events = (Event)(fd_ctx->events | event);

    // update event context
    FdContext::EventContext& event_ctx = fd_ctx->getEventContext(event);
    assert(!event_ctx.scheduler && !event_ctx.fiber && !event_ctx.cb);
    // 绑定协程或回调
    event_ctx.scheduler = Scheduler::GetThis();
    // 协程非阻塞 IO
    if (cb) 
    {
        // 如果用户提供了回调函数，就存 cb
        event_ctx.cb.swap(cb);
    } 
    else 
    {
        // 否则将当前协程注册为事件上下文
        event_ctx.fiber = Fiber::GetThis();
        assert(event_ctx.fiber->getState() == Fiber::RUNNING);
    }
    return 0;
}

bool IOManager::delEvent(int fd, Event event) {
    // attemp to find FdContext 
    FdContext *fd_ctx = nullptr;
    
    std::shared_lock<std::shared_mutex> read_lock(m_mutex);
    if ((int)m_fdContexts.size() > fd) 
    {
        fd_ctx = m_fdContexts[fd];
        read_lock.unlock();
    }
    else 
    {
        read_lock.unlock();
        return false;
    }

    std::lock_guard<std::mutex> lock(fd_ctx->mutex);

    // the event doesn't exist
    if (!(fd_ctx->events & event)) 
    {
        return false;
    }

    // delete the event
    Event new_events = (Event)(fd_ctx->events & ~event);
    int op           = new_events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
    epoll_event epevent;
    epevent.events   = EPOLLET | new_events;
    epevent.data.ptr = fd_ctx;

    int rt = epoll_ctl(m_epfd, op, fd, &epevent);
    if (rt) 
    {
        std::cerr << "delEvent::epoll_ctl failed: " << strerror(errno) << std::endl; 
        return -1;
    }


    --m_pendingEventCount;

    // update fdcontext
    fd_ctx->events = new_events;

    // update event context
    FdContext::EventContext& event_ctx = fd_ctx->getEventContext(event);
    fd_ctx->resetEventContext(event_ctx);
    return true;
}
// 取消某个文件描述符上的事件（比如读/写事件）
bool IOManager::cancelEvent(int fd, Event event) {
    // attemp to find FdContext 
    FdContext *fd_ctx = nullptr;
    
    std::shared_lock<std::shared_mutex> read_lock(m_mutex);
    if ((int)m_fdContexts.size() > fd) 
    {
        fd_ctx = m_fdContexts[fd];
        read_lock.unlock();
    }
    else 
    {
        read_lock.unlock();
        return false;
    }

    std::lock_guard<std::mutex> lock(fd_ctx->mutex);

    // the event doesn't exist
    if (!(fd_ctx->events & event)) 
    {
        return false;
    }

    // delete the event
    Event new_events = (Event)(fd_ctx->events & ~event);
    int op           = new_events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
    epoll_event epevent;
    epevent.events   = EPOLLET | new_events;
    epevent.data.ptr = fd_ctx;

    int rt = epoll_ctl(m_epfd, op, fd, &epevent);
    if (rt) 
    {
        std::cerr << "cancelEvent::epoll_ctl failed: " << strerror(errno) << std::endl; 
        return -1;
    }

    --m_pendingEventCount;

    // update fdcontext, event context and trigger
    fd_ctx->triggerEvent(event);    
    return true;
}

bool IOManager::cancelAll(int fd) {
    // attemp to find FdContext 
    FdContext *fd_ctx = nullptr;
    
    std::shared_lock<std::shared_mutex> read_lock(m_mutex);
    if ((int)m_fdContexts.size() > fd) 
    {
        fd_ctx = m_fdContexts[fd];
        read_lock.unlock();
    }
    else 
    {
        read_lock.unlock();
        return false;
    }

    std::lock_guard<std::mutex> lock(fd_ctx->mutex);
    
    // none of events exist
    if (!fd_ctx->events) 
    {
        return false;
    }

    // delete all events
    int op = EPOLL_CTL_DEL;
    epoll_event epevent;
    epevent.events   = 0;
    epevent.data.ptr = fd_ctx;

    int rt = epoll_ctl(m_epfd, op, fd, &epevent);
    if (rt) 
    {
        std::cerr << "IOManager::epoll_ctl failed: " << strerror(errno) << std::endl; 
        return -1;
    }

    // update fdcontext, event context and trigger
    if (fd_ctx->events & READ) 
    {
        fd_ctx->triggerEvent(READ);
        --m_pendingEventCount;
    }

    if (fd_ctx->events & WRITE) 
    {
        fd_ctx->triggerEvent(WRITE);
        --m_pendingEventCount;
    }

    assert(fd_ctx->events == 0);
    return true;
}

void IOManager::tickle() 
{
    // no idle threads
    if(!hasIdleThreads()) 
    {
        return;
    }
    int rt = write(m_tickleFds[1], "T", 1);
    assert(rt == 1);
}

bool IOManager::stopping() 
{
    uint64_t timeout = getNextTimer();
    // no timers left and no pending events left with the Scheduler::stopping()
    return timeout == ~0ull && m_pendingEventCount == 0 && Scheduler::stopping();
}

// 调度器的空闲协程在没有任务时运行
void IOManager::idle() 
{    
    // events 是 epoll_wait 的输出数组，用来接收就绪事件。
    static const uint64_t MAX_EVNETS = 256;
    std::unique_ptr<epoll_event[]> events(new epoll_event[MAX_EVNETS]);

    while (true) 
    {
        if(debug) std::cout << "IOManager::idle(),run in thread: " << Thread::GetThreadId() << std::endl; 

        if(stopping()) 
        {
            if(debug) std::cout << "name = " << getName() << " idle exits in thread: " << Thread::GetThreadId() << std::endl;
            break;
        }

        // blocked at epoll_wait
        int rt = 0;
        while(true)
        {
            static const uint64_t MAX_TIMEOUT = 5000;
            uint64_t next_timeout = getNextTimer();
            next_timeout = std::min(next_timeout, MAX_TIMEOUT);
            // epoll_wait 阻塞等待事件发生
            rt = epoll_wait(m_epfd, events.get(), MAX_EVNETS, (int)next_timeout);
            // EINTR -> retry，支持 信号中断重试（EINTR）
            if(rt < 0 && errno == EINTR) 
            {
                continue;
            } 
            else 
            {
                break;
            }
        };

        // collect all timers overdue，执行到期的定时器
        std::vector<std::function<void()>> cbs;
        listExpiredCb(cbs);
        if(!cbs.empty()) 
        {
            for(const auto& cb : cbs) 
            {
                scheduleLock(cb);
            }
            cbs.clear();
        }
        
        // collect all events ready处理 epoll 返回的事件
        for (int i = 0; i < rt; ++i) 
        {
            epoll_event& event = events[i];

            // tickle event：管道只起 通知作用
            // 调度器通过写入一个管道（pipe）来通知 idle 协程，有新的任务需要处理。
            // m_tickleFds[1] 写端：调度器添加任务时写入
            // m_tickleFds[0] 读端：idle 协程在 epoll_wait 返回后读取，清空管道
            if (event.data.fd == m_tickleFds[0]) 
            {
                // 边缘触发：只在状态从“无事件”到“有事件”变化时触发一次。
                uint8_t dummy[256];
                // edge triggered -> exhaust
                // 循环读取管道，直到耗尽所有数据
                while (read(m_tickleFds[0], dummy, sizeof(dummy)) > 0);
                continue;
            }

            // other events
            FdContext *fd_ctx = (FdContext *)event.data.ptr;
            std::lock_guard<std::mutex> lock(fd_ctx->mutex);

            // 将 epoll 的事件转换为 IOManager 的事件类型（READ/WRITE）
            // convert EPOLLERR or EPOLLHUP to -> read or write event
            if (event.events & (EPOLLERR | EPOLLHUP)) 
            {
                event.events |= (EPOLLIN | EPOLLOUT) & fd_ctx->events;
            }
            // events happening during this turn of epoll_wait
            int real_events = NONE;
            if (event.events & EPOLLIN) 
            {
                real_events |= READ;
            }
            if (event.events & EPOLLOUT) 
            {
                real_events |= WRITE;
            }

            if ((fd_ctx->events & real_events) == NONE) 
            {
                continue;
            }

            // 更新 epoll 状态
            // delete the events that have already happened
            int left_events = (fd_ctx->events & ~real_events);
            // 如果还有剩余事件，用 EPOLL_CTL_MOD 更新 epoll
            int op          = left_events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
            event.events    = EPOLLET | left_events;

            int rt2 = epoll_ctl(m_epfd, op, fd_ctx->fd, &event);
            if (rt2) 
            {
                std::cerr << "idle::epoll_ctl failed: " << strerror(errno) << std::endl; 
                continue;
            }

            // schedule callback and update fdcontext and event context调度协程或回调
            if (real_events & READ) 
            {
                fd_ctx->triggerEvent(READ);
                --m_pendingEventCount;
            }
            if (real_events & WRITE) 
            {
                fd_ctx->triggerEvent(WRITE);
                --m_pendingEventCount;
            }
        } // end for
        // 协程 yield
        Fiber::GetThis()->yield();
  
    } // end while(true)
}

void IOManager::onTimerInsertedAtFront() 
{
    tickle();
}

} // end namespace sylar