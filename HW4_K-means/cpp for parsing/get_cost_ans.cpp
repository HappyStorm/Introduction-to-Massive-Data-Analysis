#include <iostream>
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <cstring>
using namespace std;


int main(int argc, char const *argv[])
{
	FILE *in, *out;

	out = fopen("c1_cost.txt", "w");
	// c1: cost_c1_01.txt
	for(int i=1; i<=20; ++i){
		char in_name[200]={'\0'}, id[200]={'\0'};
		sprintf(id, "%02d.txt", i);
		strcat(in_name, "cost_c1_");
		strcat(in_name, id);
		in = fopen(in_name, "r");

		puts(in_name);
		char temp[1000];
		while(fgets(temp, 990, in)){
			if(temp[0]=='T'){
				char ans[1000];
				strcpy(ans, temp+12);
				fprintf(out, "%s", ans);
			}
		}
		fclose(in);
	}
	fclose(out);


	out = fopen("c2_cost.txt", "w");
	// c1: cost_c1_01.txt
	for(int i=1; i<=20; ++i){
		char in_name[200]={'\0'}, id[200]={'\0'};
		sprintf(id, "%02d.txt", i);
		strcat(in_name, "cost_c2_");
		strcat(in_name, id);
		in = fopen(in_name, "r");

		puts(in_name);
		char temp[1000];
		while(fgets(temp, 990, in)){
			if(temp[0]=='T'){
				char ans[1000];
				strcpy(ans, temp+12);
				fprintf(out, "%s", ans);
			}
		}
		fclose(in);
	}
	fclose(out);
	return 0;
}
