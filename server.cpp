#include <iostream>
#include <sys/socket.h>
#include <ostream>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <string>
#include <vector>
#include <future>
#include <chrono>
#include <mutex>
#include <utility>
#include <fstream>
#include <time.h>

#define PORT 80
#define MAXCONNQU 20
#define MAXFSIZE 8192
#define BUFSIZE MAXFSIZE + 1024
#define FAILEDMAXTHREADS 128

using namespace std;

std::mutex MuteResp;

enum reqType {
    GET = 1,
    POST,
    PUT,
    DELETE,
    UNKNOWN
};

typedef struct {
    bool IsCorrect;
    reqType RType;
    string Arg;
    double ProtoVers;
    string Host;
    vector<pair<string, string> > Attrib;
    string Add;
} TClReq;

string defPage = "index.html";

void ParseFirst(string& elem, TClReq& request){
    if (elem == "GET"){
        request.RType = GET;
    }
    else if (elem == "POST"){
        request.RType = POST;
    }
    else if (elem == "PUT"){
        request.RType = PUT;
    }
    else if (elem == "DELETE"){
        request.RType = DELETE;
    }
    else if (elem.substr(0, 5) == "HTTP/"){
        request.ProtoVers = stof(elem.substr(5, string::npos));
    }
    else {
        request.Arg = elem;
        if (request.Arg == "/"){
            request.Arg += defPage;
        }
    }
}

bool ParseHeadStr(string& str, TClReq& request){
    if (str.empty()){
        return false;
    }
    int pos = str.find(':',0);
    if (str.substr(0,pos) == "Host"){
        request.Host = str.substr(pos + 2, string::npos);
        if (request.Host == ""){
            request.IsCorrect = false;
        }
    }
    else {
        pair<string, string> attr;
        attr.first = str.substr(0, pos);
        attr.second = str.substr(pos + 2, string::npos);
        request.Attrib.push_back(attr);
    }
    return true;
}

int ParseRequest(int sock, TClReq& request){
    request.ProtoVers = 0.9;
    request.IsCorrect = true;
    request.RType = UNKNOWN;
    request.Host = "";
    request.Add = "";
    char buf[BUFSIZE];
    ssize_t result = recv(sock, buf, BUFSIZE, 0);
    if (result <= 0){
        close(sock);
        return result;
    }/*
    char check[2];
    ssize_t chk_res = recv(sock, check, 1, 0);
    if (chk_res != 0){
        string sprt = "\r\n";
        stringstream resp;
        resp << "HTTP/1.1 413 Request Entity Too Large" << sprt
            << sprt;
        send(sock, resp.str().c_str(), resp.str().length(), 0);
        return 0;
    }*/
    int prev, cur;
    string req(buf, result);
    prev = 0;
    cur = req.find('\n', prev);
    while (cur != (int)string::npos){
        if (prev == 0){
            int nextSpace;
            string parse;
            nextSpace = req.find(' ', prev);
            if (nextSpace >= cur || nextSpace == (int)string::npos){
                request.IsCorrect = false;
                return result;
            }
            while (nextSpace != (int)string::npos && nextSpace < cur){
                parse = req.substr(prev, nextSpace - prev);
                ParseFirst(parse, request);
                prev = nextSpace + 1;
                nextSpace = req.find(' ', prev);
            }
            parse = req.substr(prev, cur - prev);
            parse.pop_back();
            ParseFirst(parse, request);
        }
        else {
            string parse = req.substr(prev, cur - prev);
            parse.pop_back();
            if (!ParseHeadStr(parse, request)){
                request.Add = req.substr(cur + 1, string::npos);
                return result;
            }
        }
        prev = cur + 1;
        cur = req.find('\n', prev);
    }
    if (prev == 0){
        request.IsCorrect = false;
        return result;
    }
    else {
        string parse = req.substr(prev);
        if (!parse.empty())
            ParseHeadStr(parse, request);
    }
    return result;
}

void MakeOK(stringstream& resp){
    string sprt = "\r\n";
    resp << "HTTP/1.1 200 OK" << sprt << sprt;
}

void MakeNoCont(stringstream& resp){
    string sprt = "\r\n";
    resp << "HTTP/1.1 204 No Content" << sprt << sprt;
}

void MakeBadReq(stringstream& resp){
    string sprt = "\r\n";
    resp << "HTTP/1.1 400 Bad Request" << sprt << sprt;
}

void MakeNotFound(stringstream& resp){
    string sprt = "\r\n";
    resp << "HTTP/1.1 404 Not Found" << sprt << sprt;
}

void MakeLengthReq(stringstream& resp){
    string sprt = "\r\n";
    resp << "411 Length Required" << sprt << sprt;
}

void MakeNotSupp(stringstream& resp){
    string sprt = "\r\n";
    resp << "HTTP/1.1 505 HTTP Version Not Supported" << sprt << sprt;
}

void MakeIntErr(stringstream& resp){
    string sprt = "\r\n";
    resp << "HTTP/1.1 500 Internal Server Error" << sprt << sprt;
}

void MakeNotImpl(stringstream& resp){
    string sprt = "\r\n";
    resp << "HTTP/1.1 501 Not Implemented" << sprt
    << "Allow: GET, POST, PUT, DELETE" << sprt
    << sprt;
}

void MakeInsStor(stringstream& resp){
    string sprt = "\r\n";
    resp << "HTTP/1.1 507 Insufficient Storage" << sprt << sprt;
}

void MakeGet(stringstream& resp, TClReq& request){
    string path = request.Host + request.Arg;
    ifstream rFile(path);
    string sprt = "\r\n";
    string AccRng = "";
    for (size_t i = 0; i < request.Attrib.size(); i++){
        if (request.Attrib[i].first == "Range"){
            AccRng = request.Attrib[i].second;
            break;
        }
    }
    if (AccRng != ""){
        int div = AccRng.find('-',0);
        if (div == (int)string::npos){
            MakeBadReq(resp);
            rFile.close();
            return;
        }
        long long first, last;
        if (AccRng.substr(0,6) != "bytes="){
            MakeNotImpl(resp);
        }
        first = stoll(AccRng.substr(7,div - 7));
        if (AccRng.substr(div + 1, string::npos) == ""){
            last = -1;
        }
        else {
            last = stoll(AccRng.substr(div + 1, string::npos));
        }
        struct stat f;
        if ((stat(path.c_str(), &f)) == -1){
            MakeIntErr(resp);
            rFile.close();
            return;
        }
        if (last == -1){
            last = f.st_size - 1;
        }
        if (!(first == 1 && last == f.st_size - 1)){
            if (last - first + 1 > MAXFSIZE){
                MakeInsStor(resp);
                rFile.close();
                return;
            }
            resp << "HTTP/1.1 206 Partial Content" << sprt
                << "Accept-Ranges: bytes" << sprt
                << "Content-Range: bytes " << first << "-" << last << "/" << f.st_size << sprt
                << "Content-Length: " << first - last + 1 << sprt
                << "Content-Type: " << "fdfdf" << sprt
                << sprt;
            char* toSend = new char[last - first + 1];
            rFile.seekg(first - 1);
            rFile.get(toSend, last - first + 1);
            resp << toSend;
            rFile.close();
            delete toSend;
            return;
        }
    }
    struct stat f;
    if ((stat(path.c_str(), &f)) == -1){
        MakeIntErr(resp);
        rFile.close();
        return;
    }
    string content;
    while (!rFile.eof()){
        content += rFile.get();
    }
    rFile.close();
    content.pop_back();
    content.pop_back();
    resp << "HTTP/1.1 200 OK" << sprt
        << "Accept-Ranges: bytes" << sprt
        << "Content-Length: " << f.st_size << sprt
        << sprt
        << content;
}

void MakePostMsg(stringstream& resp, TClReq& request){
    ofstream mainPage(request.Host + request.Arg, fstream::app);
    mainPage.seekp(0, ios_base::end);
    time_t timePosted;
    time(&timePosted);
    struct tm *tmp;
    tmp = localtime(&timePosted);
    char timeGood[200];
    strftime(timeGood, sizeof(timeGood), "%c",tmp);
    mainPage << '\n' << "<br>"
        << "Message" << '\n' << "<br>"
        << request.Add << '\n' << "<br>"
        << "Added at " << timeGood << '\n' << "<br>";
}

void MakePost(stringstream& resp, TClReq& request){
    string contType = "";
    for (size_t i = 0; i < request.Attrib.size(); i++){
        if (request.Attrib[i].first == "Content-Type"){
            contType = request.Attrib[i].second;
        }
    }
    if (request.Arg == '/' + defPage){
        if (contType == "text/plane"){
            MakePostMsg(resp, request);
            MakeOK(resp);
            return;
        }
        else if (contType.empty()){
            MakeBadReq(resp);
            return;
        }
    }
    else {
        request.Arg = "/uploads" + request.Arg;
        if (request.Add.length() == 0){
            MakeNoCont(resp);
            return;
        }
        long long length = 0;
        for (size_t i = 0; i < request.Attrib.size(); i++){
            if (request.Attrib[i].first == "Content-Length"){
                length = stoll(request.Attrib[i].second);
            }
        }
        if (length == 0){
            MakeLengthReq(resp);
            return;
        }
        else if (length > MAXFSIZE){
            MakeInsStor(resp);
            return;
        }
        string path = request.Host + request.Arg;
        int fd = open(path.c_str(), O_WRONLY);
        if (fd == -1){
            if (errno == ENOTDIR || errno == ENOENT){
                int prev = 1, cur = 1;
                string dir = request.Host + '/';
                cur = request.Arg.find('/', prev);
                while (cur != (int)string::npos){
                    string crDir = request.Arg.substr(prev, cur - prev);
                    dir += crDir + '/';
                    if (mkdir(dir.c_str(), 777) != 0){
                        if (errno != EEXIST){
                            MakeIntErr(resp);
                            return;
                        }
                    }
                    prev = cur + 1;
                    cur = request.Arg.find('/', prev);
                }
                fd = open(path.c_str(), O_WRONLY);
                if (fd == -1){
                    fd = open(path.c_str(), O_CREAT);
                    close(fd);
                    fd = open(path.c_str(), O_WRONLY);
                }
            }
            else {
                MakeIntErr(resp);
                return;
            }
        }
        int lRng = 1;
        for (size_t i = 0; i < request.Attrib.size(); i++){
            if (request.Attrib[i].first == "Content-Range"){
                if (request.Attrib[i].second.substr(0,6) != "bytes "){
                    close(fd);
                    MakeNotImpl(resp);
                    return;
                }
                int div = request.Attrib[i].second.find('-',0);
                if (div == (int)string::npos){
                    close(fd);
                    MakeBadReq(resp);
                    return;
                }
                lRng = stoi(request.Attrib[i].second.substr(7, div - 7));
            }
        }
        if (lRng != 1){
            if (lseek(fd, lRng - 1, SEEK_SET)){
                MakeIntErr(resp);
                close(fd);
                return;
            }
        }
        write(fd, request.Add.c_str(), length + 1);
        string sprt = "\r\n";
        resp << "HTTP/1.1 201 Created" << sprt
            << "Location: " << path << sprt
            << sprt;
    }
}

void MakePut(stringstream& resp, TClReq& request){
    if (request.Arg == '/' + defPage){
        string sprt = "\r\n";
        resp << "HTTP/1.1 405 Method Not Allowed" << sprt
            << "Allow: GET, POST" << sprt
            << sprt;
        return;
    }
    else {
        if (request.Add.length() == 0){
            MakeNoCont(resp);
            return;
        }
        long long length = 0;
        for (size_t i = 0; i < request.Attrib.size(); i++){
            if (request.Attrib[i].first == "Content-Length"){
                length = stoll(request.Attrib[i].second);
            }
        }
        if (length == 0){
            MakeLengthReq(resp);
            return;
        }
        else if (length > MAXFSIZE){
            MakeInsStor(resp);
            return;
        }
        string path = request.Host + request.Arg;
        int fd = open(path.c_str(), O_WRONLY);
        if (fd == -1){
            if (errno == ENOTDIR || errno == ENOENT){
                int prev = 1, cur = 1;
                string dir = request.Host + '/';
                cur = request.Arg.find('/', prev);
                while (cur != (int)string::npos){
                    string crDir = request.Arg.substr(prev, cur - prev);
                    dir += crDir + '/';
                    if (mkdir(dir.c_str(), 0777) != 0){
                        if (errno != EEXIST){
                            MakeIntErr(resp);
                            return;
                        }
                    }
                    prev = cur + 1;
                    cur = request.Arg.find('/', prev);
                }
                fd = open(path.c_str(), O_WRONLY);
                if (fd == -1){
                    fd = open(path.c_str(), O_CREAT);
                    close(fd);
                    fd = open(path.c_str(), O_WRONLY);
                }
            }
            else {
                MakeIntErr(resp);
                return;
            }
        }
        int lRng = 1;
        for (size_t i = 0; i < request.Attrib.size(); i++){
            if (request.Attrib[i].first == "Content-Range"){
                if (request.Attrib[i].second.substr(0,6) != "bytes "){
                    close(fd);
                    MakeNotImpl(resp);
                    return;
                }
                int div = request.Attrib[i].second.find('-',0);
                if (div == (int)string::npos){
                    close(fd);
                    MakeBadReq(resp);
                    return;
                }
                lRng = stoi(request.Attrib[i].second.substr(7, div - 7));
            }
        }
        if (lRng != 1){
            if (lseek(fd, lRng - 1, SEEK_SET)){
                MakeIntErr(resp);
                close(fd);
                return;
            }
        }
        write(fd, request.Add.c_str(), length + 1);
        string sprt = "\r\n";
        resp << "HTTP/1.1 201 Created" << sprt
            << "Location: " << path << sprt
            << sprt;
    }
}

void MakeDelete(stringstream& resp, TClReq& request){
    if (request.Arg == '/' + defPage){
        string sprt = "\r\n";
        resp << "HTTP/1.1 405 Method Not Allowed" << sprt
            << "Allow: GET, POST" << sprt
            << sprt;
        return;
    }
    else {
        string path = request.Host + request.Arg;
        if (remove(path.c_str()) == -1){
            MakeIntErr(resp);
            return;
        }
        MakeOK(resp);
        return;
    }
}

int SendResponse(int sock, TClReq& request){
    try {
        stringstream resp;
        stringstream respBody;
        if (!request.IsCorrect || request.RType == UNKNOWN){
            MakeBadReq(resp);
        }
        else if (request.ProtoVers < 1.1){
            MakeNotSupp(resp);
        }
        else {
            switch (request.RType){
            case GET: {
                string path = request.Host + request.Arg;
                ifstream reqFile(path);
                if (!reqFile.is_open()){
                    MakeNotFound(resp);
                }
                else {
                    reqFile.close();
                    MakeGet(resp,request);
                }
            }
            break;
            case POST:{
                MakePost(resp, request);
            }
            break;
            case PUT:{
                MakePut(resp, request);
            }
            break;
            case DELETE:{
                MakeDelete(resp, request);
            }
            break;
            case UNKNOWN:{
                MakeNotImpl(resp);
            }
            break;
            }
        }
        ssize_t res = send(sock, resp.str().c_str(), resp.str().length(), 0);
        if (res == -1){
            perror("send response");
            return errno;
        }
        return 0;
    }
    catch (std::exception e){
        dprintf(STDERR_FILENO, "%s\n", e.what());
        dprintf(STDERR_FILENO, "Request type: %d\n", request.RType);
        stringstream resp;
        MakeIntErr(resp);
        ssize_t res = send(sock, resp.str().c_str(), resp.str().length(), 0);
        if (res == -1){
            perror("send response");
            return 0;
        }
        return -1;
    }
}

int ServeClient(int sockHost){
    int retStat = 0;
    int sockCl = accept(sockHost, 0, 0);
    if (sockCl < 0) {
        perror("accept");
        //close(sockHost);
        retStat = errno;
        errno = 0;
        return retStat;
    }
    TClReq request;
    int res = ParseRequest(sockCl, request);
    if (res == 0){
        close(sockCl);
        retStat = errno;
        errno = 0;
        return retStat;
    }
    else if (res < 0){
        perror("parse request");
        close(sockCl);
        retStat = errno;
        errno = 0;
        return retStat;
    }
    int respRes = 0;
    {
        std::lock_guard<std::mutex> lock(MuteResp);
        respRes = SendResponse(sockCl, request);
    }
    shutdown(sockCl, SHUT_RDWR);
    close(sockCl);
    return respRes;
}

void Quit(void){
    char c = '\0';
    while (c != 'q'){
        dprintf(STDERR_FILENO, "Type \"q\" to kill server.\n");
        cin >> c;
        if (c != 'q'){
            char buf[128];
            cin.getline(buf, 128, '\n');
        }
    }
}

int main() {
    try {
        struct sockaddr_in sockHttpAddr;
        int sockHost = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (sockHost == -1) {
            perror("socket");
            exit(errno);
        }
        memset(&sockHttpAddr, 0, sizeof (sockHttpAddr));
        sockHttpAddr.sin_family = PF_INET;
        sockHttpAddr.sin_port = htons(PORT);
        sockHttpAddr.sin_addr.s_addr = htonl(INADDR_ANY);
        int enable = 1;
        if (setsockopt(sockHost, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) == -1){
            perror("setsockopt(SO_REUSEADDR)");
            exit(errno);
        }
        if (bind(sockHost, (struct sockaddr*) &sockHttpAddr, sizeof (sockHttpAddr)) == -1) {
            perror("bind");
            close(sockHost);
            exit(errno);
        }
        if (listen(sockHost, MAXCONNQU) == -1) {
            perror("listen");
            close(sockHost);
            exit(errno);
        }
        struct rlimit* sysInfo = new struct rlimit;
        int nThreads;
        if (getrlimit(RLIMIT_NPROC, sysInfo) == -1){
            nThreads = FAILEDMAXTHREADS;
            dprintf(STDERR_FILENO, "Couldn't get maximum number of threads for this system.\n");
            dprintf(STDERR_FILENO, "Number of threads is set to %d", nThreads);
        }
        else {
            nThreads = sysInfo->rlim_max / 16;      //magic number
        }
        delete sysInfo;
        std::future<int>* thrdPool = new std::future<int>[nThreads];
        for (int i = 0; i < nThreads; i++){
            thrdPool[i] = std::async(std::launch::async, ServeClient, sockHost);
        }
        dprintf(STDERR_FILENO, "Server is running.\n");
        std::future<void> serverQuit = std::async(std::launch::async, Quit);
        while(true){
            for (int i = 0; i < nThreads; i++){
                std::future_status status;
                status = thrdPool[i].wait_for(std::chrono::milliseconds(10));
                if (status == std::future_status::ready){
                    int res = thrdPool[i].get();
                    if (res > 0){
                        errno = res;
                        perror("ServeClient");
                        errno = 0;
                    }
                    thrdPool[i] = std::async(std::launch::async, ServeClient, sockHost);
                }
                status = serverQuit.wait_for(std::chrono::milliseconds(5));
                if (status == std::future_status::ready){
                    exit(0);
                }
            }
        }
        delete[] thrdPool;
        close(sockHost);
        return 0;
    }
    catch (std::bad_alloc b){
        dprintf(STDERR_FILENO, "bad alloc happened\n");
        return EFBIG;
    }
}
