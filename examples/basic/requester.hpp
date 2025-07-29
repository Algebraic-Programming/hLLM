#include <queue>
#include <mutex>
#include "hllm/engine.hpp"

// hLLM Engine object
hLLM::Engine* _engine;

// Request count to produce
size_t _requestCount; 

// Request count finished
size_t _requestsFinished; 

// Type definition for request ids
typedef size_t requestId_t;

// Buffer for externally produced incoming requests
std::queue<requestId_t> requests;
std::mutex requestMutex;

inline bool listenRequest(requestId_t& requestId)
{
    bool receivedRequest = false;

    requestMutex.lock();
    if (requests.empty() == false)
    {
        requestId = requests.front();
        requests.pop();
        receivedRequest = true;
    }
    requestMutex.unlock();

    return receivedRequest;
}

inline void initializeRequestServer(hLLM::Engine* engine, const size_t requestCount)
{
    _engine = engine;
    _requestCount = requestCount;
    _requestsFinished = 0;
}

inline void startRequestServer(size_t delayMs)
{
   for (size_t i = 0; i < _requestCount; i++)
   {
      requestId_t requestId = i + 1;
      requestMutex.lock();
      requests.push(requestId);
      requestMutex.unlock();

      // Don't allow it to produce many requests before the previous are consumed
      usleep(delayMs * 1000ul);
   }
}

inline void respondRequest(const std::string& response)
{
    printf("Response received: '%s'\n", response.c_str());
    _requestsFinished++;
    if (_requestsFinished == _requestCount) 
    {
        printf("Finished processing all requests.\n");
        _engine->terminate();
    }
}