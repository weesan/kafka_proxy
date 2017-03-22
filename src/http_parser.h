#ifndef HTTP_PARSER_H
#define HTTP_PARSER_H

#include <string>
#include <unordered_map>

using namespace std;

typedef unordered_map<string, string> HttpRequestBase;

class HttpRequest : public HttpRequestBase {
private:
    HttpRequestBase _params;

public:
    HttpRequestBase &operator()() {
        return _params;
    }
    void dump(void) {
        for (const_iterator itr = begin(); itr != end(); ++itr) {
            printf("%s: %s\n", itr->first.c_str(), itr->second.c_str());
        }
        for (const_iterator itr = _params.begin();
             itr != _params.end(); ++itr) {
            printf("  %s: %s\n", itr->first.c_str(), itr->second.c_str());
        }
        
    }
};

bool http_parser(const char *req, int len, HttpRequest &request);

#endif // HTTP_PARSER_H
