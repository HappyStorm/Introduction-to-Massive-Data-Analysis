#include <iostream>
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <cstring>
using namespace std;


int main(int argc, char const *argv[])
{
	FILE *in, *out;
	vector<double> v[10];

	in = fopen("c2_20.txt", "r");
	out = fopen("c2_20_ans.txt", "w");
	for(int i=0; i<10; ++i){
		int clusterID;
		fscanf(in, "%d\t", &clusterID);
		for(int j=0; j<58; ++j){
			double temp;
			fscanf(in, "%lf ", &temp);
			v[clusterID].push_back(temp);
		}
	}
	fclose(in);

	fprintf(out, "    Cluster%2d", 1);
	for(int i=2; i<=10; ++i)
		fprintf(out, "      Cluster%2d", i);
	fprintf(out, "\n");

	for(int i=0; i<58; ++i){
		fprintf(out, "%13.7lf", v[0][i]);
		for(int j=1; j<10; ++j)
			fprintf(out, "  %13.7lf", v[j][i]);
		fprintf(out, "\n");
	}
	fclose(out);
	return 0;
}
