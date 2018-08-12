/*
*    This program is free software: you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation, either version 3 of the License, or
*    (at your option) any later version.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    ALLAN MATHEUS				JANUARY 2018
*/

#include <array>
#include <omp.h>
#include "staging.h"
#define N 100000

void load_subtars_parallel(const char * arg)
{
    #define MAX_I 50
    #define MAX_J 100
    
    std::shared_ptr<staging::server> st1;
    st1 = std::make_shared<staging::server>(arg);
    st1->run_savime("create_tar('testar', '*', 'implicit,x,int,0,1000,1 "
                                            " | implicit,y,int,0,*,1 "
                                            " | implicit,z,int,0,1000,1 ', "
                                            " 'a,double|b,double');");
    
    omp_set_num_threads(4);
    #pragma omp parallel for
    for(int i = 0; i < MAX_I; i++)
    {
        std::shared_ptr<staging::server> st;
        st = std::make_shared<staging::server>(arg);
            
        for(int j = 0; j < MAX_J; j++)
        {
            int32_t id = i*MAX_J+j;
            std::string dsName = "base"+std::to_string(id);
            std::string idx = std::to_string(id);

            double _v[N];
            for(int32_t j = 0; j < N; j++)
                _v[j] = i;

            staging::dataset dataset{dsName, "double", *st.get()};
            dataset.write((char*)_v, N * sizeof(double)); 
            st->sync();


            st->run_savime("load_subtar('testar', 'ordered,x,0,99 "
                                      "| ordered,y, "+idx+","+idx+" "
                                      "| ordered,z,0,999', "
                                      " 'a,"+dsName+" | b, "+dsName+"');");
        }
        
        st->finalize();
    }   
    
}

int main(int argc, char **argv)
{
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " ADDR:PORT\n";
        return 1;
    }

    load_subtars_parallel(argv[1]);
    return 0;
}

