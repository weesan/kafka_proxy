#include <sstream>
#include "http_parser.h"

/*
 * Input: GET /_consume?gid=foo&topic=bar&size=3 HTTP/1.1
 * Output: A unordered_map as follows:
 *   {
 *       "method":  "GET",
 *       "query":   "/_consume?gid=foo&topic=foo&size=3",
 *       "version": "HTTP/1.1",
 *       "()":
 *           {
 *               "gid":   "foo",
 *               "topic": "bar",
 *               "size":  "3"
 *           }
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

    request["method"] = method;
    request["query"] = query;
    request["version"] = version;
    
    // Reset iss to the query string.
    iss.clear();
    iss.str(query);

    // Get the url without the param parts.
    string url;
    if (!getline(iss, url, '?')) {
        return false;
    }

    // Parse the params.
    string keyval, key, val;
    while (getline(iss, keyval, '&')) {
        istringstream iss(keyval);
        if (getline(getline(iss, key, '='), val)) {
            request()[key] = val;
        }
    }

    return true;
}
