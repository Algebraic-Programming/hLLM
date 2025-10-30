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
  if (res) {
      // Print HTTP status code (e.g., 200)
      std::cout << "Status code: " << res->status << std::endl;

      // Print body text returned by server
      std::cout << "Response: " << res->body << std::endl;
  } else {
      // Print an error message if connection failed
      std::cerr << "Failed to connect to server.\n";
  }
}

class RealTimeAnalysis
{
  public:

  RealTimeAnalysis(const std::string ip = "0.0.0.0", const size_t port = 5004) : _ip(ip), _port(port)
  {

    _svr.Post("/data", [this](const httplib::Request &req, httplib::Response &res) {
        {
            // Acquire lock before modifying the shared global variable.
            // The lock is automatically released when this scope ends.
            std::lock_guard<std::mutex> lock(_mtx);
            _last_received_json = req.body;
        }

        // Print received JSON to the console for visibility/debugging.
        std::cout << "Received JSON:\n" << req.body << std::endl;

        // Return an acknowledgment response to the client.
        // This shows that the server successfully processed the POST.
        // (This is for debugging)
        res.set_content("{\"status\":\"ok\"}", "application/json");
    });

    // Start listening from a separate thread
    // Default: Listen on all available network interfaces (0.0.0.0) at port 5004.
    std::thread srv_listen([this](){
      _svr.listen(_ip, _port);
    });
  }

  ~RealTimeAnalysis() = default;

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
};

}