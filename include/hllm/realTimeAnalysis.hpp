#pragma once

#include "../../extern/cpp-httplib/httplib.h"  // Lightweight HTTP server library
#include <atomic>     // For atomic variables (thread-safe counters, flags)
#include <chrono>     // For timing utilities (steady clock, seconds, etc.)
#include <mutex>      // For std::mutex and std::lock_guard (thread safety)
#include <string>     // For storing the json file
#include <iostream>   // For debugging
#include <thread>

#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>

namespace hLLM
{

/**
 * A function for a client passing its information
 * (PROTOTYPE)
 */
void clientPost(const size_t& InstanceId, const int& n_requests, httplib::Client cli)
{
  // Preparing the data to be passed over. It should only consist of a part of the whole json file
  std::string json_data =
      "{\n"
      "  \"Instance\": {\n"
      "  \"instance ID\": " + std::to_string(InstanceId) + ",\n"
      "  \"status\": \"active\",\n"
      "  \"number of requests per ms\": " + std::to_string(n_requests) + "\n"
      "  }\n"
      "}";

  // Send Post of the new json_data
  auto res = cli.Post("/data", json_data, "application/json");

  // Handle server response or connection failure
  if (!res) {
    // Print an error message if connection failed
    std::cerr << "Failed to connect to server.\n";
  }
}

class RealTimeAnalysis
{
  public:

  RealTimeAnalysis(const std::string ip = "0.0.0.0", const size_t port = 5003) : _ip(ip), _port(port)
  {
    _svr.Post("/data", [this](const httplib::Request &req, httplib::Response &res) {
        {
            // Acquire lock before modifying the shared global variable.
            // The lock is automatically released when this scope ends.
            std::lock_guard<std::mutex> lock(_mtx);
            _last_received_json = req.body;
        }

        // (Debugging)
        // Return an acknowledgment response to the client.
        // This shows that the server successfully processed the POST.
        // res.set_content("{\"status\":\"ok\"}", "application/json");
    });

    // Get method of posting the captured json file
    _svr.Get("/", [this](const httplib::Request &, httplib::Response &res) {
        // Acquire the same mutex before reading shared state.
        std::lock_guard<std::mutex> lock(_mtx);

        // Send HTML response to the browser.
        res.set_content(_last_received_json, "application/json");
    });

    // Start listening from a separate thread
    // Default: Listen on all available network interfaces (0.0.0.0) at port 5003.
    _srv_thread = std::thread([this]() {
      _svr.listen(_ip, _port);
    });
  }

  ~RealTimeAnalysis()
  {
    _svr.stop();          // tell the server to stop listening
    if (_srv_thread.joinable())
      _srv_thread.join();  // clean shutdown
  }

  private:

  /**
   * HTTP server instance
   */
  httplib::Server _svr;

  /**
   * Mutex to protect the access to the JSON file
   */
  std::mutex _mtx;

  /**
   * The received JSON file from any client
   */
  std::string _last_received_json;

  /**
   * The IP we launch our http application
   */
  std::string _ip;

  /**
   * The Port of the IP
   */
  size_t _port;

  /**
   * The server thread to run the "listen" method
   */
  std::thread _srv_thread;
};

}