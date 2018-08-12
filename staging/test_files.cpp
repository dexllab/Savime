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

#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <list>
#include "../mapped_memory/mapped_memory.h"
#include "staging.h"

std::string get_name(const std::string& path)
{
    const auto last_slash = path.find_last_of('/');
    return path.substr(last_slash + 1);
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        std::cerr << "usage: " << argv[0] << " ADDR:PORT FILE...\n";
        return 1;
    }

    staging::server st{argv[1], 1};

    std::list<mapped_memory> buffers;

    for (std::size_t i = 2; i < argc; ++i) {
        auto path = argv[i];
        auto name = get_name(path);

        staging::dataset dataset{name, "double", st};

        //std::cerr << "reading " << path << '\n';
        mapped_memory buffer{path};

        //std::cerr << "sending " << name << '\n';
        dataset.write(buffer.get(), buffer.size());

        buffers.push_back(std::move(buffer));
    }

    //std::cerr << "syncing...\n";
    st.sync();

    return 0;
}
