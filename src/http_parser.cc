#include <sstream>
#include <string.h>
#include "http_parser.h"

/*
 * Input: GET /_consume?gid=foo&topic=bar&size=3 HTTP/1.1
 * Output: An unordered_map as follows:
 *   {
 *       "_method_":  "GET",
 *       "_query_":   "/_consume?gid=bar&topic=foo&size=3",
 *       "_version_": "HTTP/1.1",
 *       "()":
 *           {
 *               "gid":   "bar",
 *               "topic": "foo",
 *               "size":  "3"
 *           }
 *   }
 *
 * Input: POST /_delete?gid=bar&topic=foo HTTP/1.1
 * 0 2
 * 1 4
 *
 * Output: An unordered_map as follows:
 *   {
 *       "_method_":  "POST",
 *       "_query_":   "/_delete?gid=bar&topic=foo",
 *       "_version_": "HTTP/1.1",
 *       "()":
 *           {
 *               "gid":   "bar",
 *               "topic": "foo"
 *           },
 *       "_data_": "0 2\n1 4\n"
 *   }
 */
bool http_parser (const char *req, int len, HttpRequest &request)
{
    istringstream iss(req);
    string method, query, version;

    //printf("Request: [%.*s]\n", len, req);
    
    // Seperate method, query and version.
    if (!getline(getline(getline(iss,
                                 method, ' '),
                         query, ' '),
                 version, '\r')) {
        return false;
    }

    request["_method_"] = method;
    request["_query_"] = query;
    request["_version_"] = version;
    
    // Reset iss to the query string.
    istringstream q_iss(query);

    // Get the url without the param parts.
    string url;
    if (!getline(q_iss, url, '?')) {
        return false;
    }

    // Parse the params.
    string keyval, key, val;
    while (getline(q_iss, keyval, '&')) {
        istringstream q_iss(keyval);
        if (getline(getline(q_iss, key, '='), val)) {
            request()[key] = val;
        }
    }

    // Data if the method is "POST"
    if ((method[0] == 'P' || method[0] == 'p') &&
        strcasecmp(method.c_str(), "POST") == 0) {
        string delim = "\r\n\r\n";
        string data(req, len);
        size_t n = data.find(delim);
        if (n != string::npos) {
            request["_data_"] = data.substr(n + delim.size());
        }
    }
    
    return true;
}
