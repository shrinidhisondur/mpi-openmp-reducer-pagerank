#include <iostream>
#include <omp.h>
#include <fstream>
#include <string>
#include <map>
#include <vector>
#include <cstdlib>
#include <cmath>

using namespace std;

class PageRank {
    
    ifstream reader;
    vector<double> pr;
    //float **Adjacency;
    vector<vector<double> > adj;
    long N;
    map<int,int> m;
    map <int, int> entry;
    map <int, int> reverse;
    int countError;

    public:
    PageRank (const char* fileName) : reader(fileName) {N=0;}

    void init () {
        string line;
        if (reader.is_open()) {
            while (getline (reader,line)) {
                char *cstr;
                cstr = const_cast<char*>(line.c_str());
                int a = strtol(cstr,&cstr, 10);
                if (m.find(a) == m.end()) {
                    m[a] = 1;
                    entry[a] = N;
                    reverse[N] = a;
                    const vector<double> temp;
                    adj.resize(entry[a]+1);
                    adj[entry[a]] = temp;
                    N++;
                } else {
                    m[a] = m[a] + 1;
                }
                int b = strtol(cstr,&cstr, 10);
                if (m.find(b) == m.end()) {
                    entry[b] = N;
                    reverse[N] = b;
                    const vector<double> temp;
                    adj.resize(entry[b]+1);
                    adj[entry[b]] = temp;
                    m[b] = 1;
                    N++;
                } else {
                    m[b] = m[b] + 1;
                }

                adj[entry[b]].resize(entry[a]+1);
                adj[entry[a]].resize(entry[b]+1);
                adj[entry[b]][entry[a]] = 1;
                adj[entry[a]][entry[b]] = 1;
            }
            reader.close();
        } else {
            cout << "Kya time pass" << endl;
        }

        pr.resize(N);
        int chunk = N/10;

        omp_set_num_threads(10);
#pragma omp parallel //shared(a,b,c,chunk) private(i)
        {
            #pragma omp for schedule(dynamic,chunk) nowait
            for (int i = 0; i < N; i++) {
                adj[i].resize(N);
                vector <double> temp = adj[i];
                pr[i] = 1/(double)N;
                for (int j = 0; j < N; j++) {
                    if (temp[j] == 1) 
                        temp[j] = 1/(double)(m[reverse[j]]);
                }
                adj[i] = temp;
            }
        }

    }

    int iterate (double error) {
        countError = 0;
        vector <double> new_pr(N);
        int chunk = N/10;

#pragma omp parallel
        {
            #pragma omp for schedule(dynamic,chunk) nowait
            for (int i = 0; i < N; i++) {
                vector <double> temp = adj[i];
                double sum = 0;
                for (int j = 0; j < N; j++) {
                    sum += temp[j]*pr[j];
                }
                new_pr[i] = 0.15*(1/(double)N) + 0.85*sum;
                if (fabs(new_pr[i] - pr[i]) > error) {
                    countError++;
                }
            }
        }
        pr = new_pr;
        return countError;
    }

    void printPr (const char* fileName) {
        ofstream myfile (fileName);
        for (int i = 0; i < N; i++) {
            //cout << entry[i] << " " << pr[entry[i]]  << endl;
            myfile << i << " " << pr[i]  << endl;
        }
    }
};

#define THRESHOLD 0.0000001

int main () { 

    PageRank* pr = new PageRank("facebook_combined.txt");

    pr->init();
    int count;
    int iter = 0;
    do {
        count = pr->iterate(THRESHOLD);
        iter++;
    } while (count > 0);
    cout << "Threshold Set = " << THRESHOLD << " Number of iterations: " << iter << endl;
    pr->printPr("Output_Task1.txt");
    return 1;
}
