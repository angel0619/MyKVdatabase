#ifndef _FD_MANAGER_H_
#define _FD_MANAGER_H_

#include <memory>
#include <shared_mutex>
#include "thread.h"


namespace sylar{

/* fd info 文件描述符上下文
	在 hook 系统调用时，知道这个 fd 的额外状态
	（是否 socket、是否非阻塞、超时设置等），
	从而把阻塞式 API改造成可挂起的协程 IO，并且对用户保持 API 行为一致。
*/ 
class FdCtx : public std::enable_shared_from_this<FdCtx>
{
private:
	bool m_isInit = false;
	bool m_isSocket = false;
	bool m_sysNonblock = false;  // 系统级非阻塞
	bool m_userNonblock = false; // 用户级非阻塞
	bool m_isClosed = false;	 // fd 是否已经被关闭
	int m_fd;

	// read event timeout
	uint64_t m_recvTimeout = (uint64_t)-1;
	// write event timeout
	uint64_t m_sendTimeout = (uint64_t)-1;

public:
	FdCtx(int fd);
	~FdCtx();

	bool init();
	bool isInit() const {return m_isInit;}
	bool isSocket() const {return m_isSocket;}
	bool isClosed() const {return m_isClosed;}

	void setUserNonblock(bool v) {m_userNonblock = v;}
	bool getUserNonblock() const {return m_userNonblock;}

	void setSysNonblock(bool v) {m_sysNonblock = v;}
	bool getSysNonblock() const {return m_sysNonblock;}

	void setTimeout(int type, uint64_t v);
	uint64_t getTimeout(int type);
};

class FdManager
{
public:
	FdManager();

	std::shared_ptr<FdCtx> get(int fd, bool auto_create = false);
	void del(int fd);

private:
	std::shared_mutex m_mutex;
	std::vector<std::shared_ptr<FdCtx>> m_datas;
};


template<typename T>
class Singleton
{
private:
    static T* instance;
    static std::mutex mutex;

protected:
    Singleton() {}  

public:
    // Delete copy constructor and assignment operation
    Singleton(const Singleton&) = delete;
    Singleton& operator=(const Singleton&) = delete;

    static T* GetInstance() 
    {
        std::lock_guard<std::mutex> lock(mutex); // Ensure thread safety
        if (instance == nullptr) 
        {
            instance = new T();
        }
        return instance;
    }

    static void DestroyInstance() 
    {
        std::lock_guard<std::mutex> lock(mutex);
        delete instance;
        instance = nullptr;
    }
};

typedef Singleton<FdManager> FdMgr;

}

#endif