
/*
*    This program is free software: you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation, either version 3 of the License, or
*    (at your option) any later version.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    HERMANO L. S. LUSTOSA				JANUARY 2018
*/

#include "test_queries_creator.h"

std::vector<std::string> TAR_NAMES = {
    "io", "ip", "it", "eo", "ep", "et"
};

std::vector<std::vector<std::string>> TAR_NAMES_PAIR = {
    {"io", "io"},
    {"io", "ip"},
    {"io", "it"},
    {"io", "eo"},
    {"io", "ep"},
    {"io", "et"},
    {"ip", "et"},
    {"it", "ep"},
    {"it", "et"},
};

std::list<std::string> SETUP_QUERIES = {
    R"(create_dataset("base:double", literal(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)))",
    R"(create_dataset("dsexplict:float", literal(1.5, 3.2, 4.7, 7.9, 13.1)))",
    R"(create_dataset("dspart1:int", "2:2:6:1"))",
    R"(create_dataset("dspart2:int", literal(2, 8)))",
    R"(create_dataset("dsepart1:long", literal(3, 1, 4)))",
    R"(create_dataset("dsepart2:long", literal(1, 2)))",
    R"(create_dataset("dstotalimplicitx:int", literal(2, 10, 4, 2, 6, 8)))",
    R"(create_dataset("dstotalimplicity:int", literal(2, 2, 4, 8, 8, 10)))",
    R"(create_dataset("dstotalexplicitx:long", literal(4, 2, 3, 1, 0)))",
    R"(create_dataset("dstotalexplicity:long", literal(0, 2, 3, 2, 4)))",
    R"(create_dataset("vec:int:2", literal(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))",
    R"(create_dataset("vecstr:char:10", literal("apples", "oranges", "bananas", "melons", "pears", "peaches")))",
    R"(create_type("footype(dim1, dim2, val"))",
    R"(create_tar("io", "footype", "implicit,x,int,0,10,1 | implicit,y,int,0,10,1", "a,double", "x, dim1, y, dim2, a, val"))",
    R"(create_tar("io2", "footype", "implicit,x,int,0,10,2 | implicit,y,int,0,10,2", "a,double", "x, dim1, y, dim2, a, val"))",
    R"(create_tar("iohalf", "footype", "implicit,x,float,0,10,0.5 | implicit,y,float,0,10,0.5", "a,double", "x, dim1, y, dim2, a, val"))",
    R"(create_tar("ip", "*", "implicit, x, int,0,10,2 | implicit, y, int,0,10,2", "a,double"))",
    R"(create_tar("it", "*", "implicit, x, int,0,10,2 | implicit, y, int,0,10,2", "a,double"))",
    R"(create_tar("eo", "*", "explicit, x, dsexplict  | explicit, y, dsexplict", "a,double"))",
    R"(create_tar("ep", "*", "explicit, x, dsexplict  | explicit, y, dsexplict", "a,double"))",
    R"(create_tar("et", "*", "explicit, x, dsexplict  | explicit, y, dsexplict", "a,double"))",
    R"(create_tar("vi", "*", "implicit, i, int, 1, 5, 1", "a,int:2"))",
    R"(create_tar("vs", "*", "implicit, i, int, 0, 5, 1", "s,char:10"))",
    R"(load_subtar("io", "ordered, x, 0, 2  | ordered, y, 0, 2",  "a,base"))",
    R"(load_subtar("io", "ordered, x, 0, 2  | ordered, y, 3, 5", "a,base"))",
    R"(load_subtar("io", "ordered, x, 3, 5 | ordered, y, 0, 2",  "a,base"))",
    R"(load_subtar("io", "ordered, x, 3, 5 | ordered, y, 3, 5", "a,base"))",
    R"(load_subtar("io2", "ordered, x, #0, #4  | ordered, y, #0, #4",  "a,base"))",
    R"(load_subtar("io2", "ordered, x, #0, #4  | ordered, y, #6, #10", "a,base"))",
    R"(load_subtar("io2", "ordered, x, #6, #10 | ordered, y, #0, #4",  "a,base"))",
    R"(load_subtar("io2", "ordered, x, #6, #10 | ordered, y, #6, #10", "a,base"))",
    R"(load_subtar("iohalf", "ordered, x, 0, 2  | ordered, y, 0, 2",  "a,base"))",
    R"(load_subtar("iohalf", "ordered, x, 0, 2  | ordered, y, 3, 5", "a,base"))",
    R"(load_subtar("iohalf", "ordered, x, 3, 5 | ordered, y, 0, 2",  "a,base"))",
    R"(load_subtar("iohalf", "ordered, x, 3, 5 | ordered, y, 3, 5", "a,base"))",
    R"(load_subtar("eo", "ordered, x, #1.5, #4.7  | ordered, y, #1.5, #4.7", "a,base"))",
    R"(load_subtar("eo", "ordered, x, #1.5, #4.7  | ordered, y, #7.9, #13.1", "a,base"))",
    R"(load_subtar("eo", "ordered, x, #7.9, #13.1  | ordered, y, #1.5, #4.7", "a,base"))",
    R"(load_subtar("eo", "ordered, x, #7.9, #13.1  | ordered, y, #7.9, #13.1", "a,base"))",
    R"(load_subtar("ip", "ordered, x, #0, #6 | partial, y, #0, #10, dspart1", "a,base"))",
    R"(load_subtar("ip", "ordered, x, #8, #10 | partial, y, #0, #10, dspart2", "a,base"))",
    R"(load_subtar("ep", "ordered, x, #1.5, #4.7 | partial, y, #1.5, #13.1, dsepart1", "a,base"))",
    R"(load_subtar("ep", "ordered, x, #7.9, #13.1 | partial, y, #1.5, #13.1, dsepart2", "a,base"))",
    R"(load_subtar("it", "total, x, #0, #10, dstotalimplicitx | total, y, #0, #10, dstotalimplicity", "a,base"))",
    R"(load_subtar("et", "total, x, #1.5, #13.1, dstotalexplicitx | total, y, #1.5, #13.1, dstotalexplicity", "a,base"))",
    R"(load_subtar("vi", "ordered, i, 0, 4", "a,vec"))",
    R"(load_subtar("vs", "ordered, i, 0, 5", "s,vecstr"))"
};

std::list<std::list<std::string>> ONE_TAR_QUERIES = {
    {"select(@param1, x, y, a)", "select(@param1, x, a)"},
    {"subset(@param1, x, 2, 4)", "subset(@param1, x, 2, 4, y, 2, 2)",
     "subset(@param1, x, 1.2, 3.4, y, 0.732, 1.449)",
     "subset(@param1, x, 1, 1, y, 2, 10)", "subset(@param1, x, 1, 2)",
     "subset(@param1, x, 3, 4)", "subset(@param1, y, 2, 6)",
     "subset(@param1, x, 2, 10)"},
    {"where(@param1, a >= 4 and a <= 8)", "where(@param1, x = 2 or x = 8)",
     "where(@param1, not (a % 2) = 0)", "where(@param1, x = y)",
     "where(@param1, x < y)", "where(@param1, x^2 < 16)"},
    {"derive(@param1, derived, x+1)", "derive(@param1, derived, a/1)",
     "derive(@param1, derived, (x+a)*2)",
     "derive(@param1, derived, sqrt(x)+3)",
     "derive(@param1, derived, sin(a)*cos(a))",
     "derive(@param1, derived, floor(x)*a)"},
    {"aggregate(@param1, avg, a, avg_a)",
     "aggregate(@param1, count, a, c_a, x)",
     "aggregate(@param1, max, a, max_a, x)",
     "aggregate(@param1, count, x, agg, x, y)",
     "aggregate(@param1, sum, y, sum_y, x)",
     "aggregate(@param1, avg, y, avg_y, y)"}};

std::list<std::list<std::string>> REDUCED_ONE_TAR_QUERIES_INNER = {
    {"select(@param1, x, a)"},
    {"subset(@param1, x, 2, 4)"},
    {"where(@param1, a >= 2 and a <= 8)"},
    {"derive(@param1, derived_outer, x+1)"},
    {"aggregate(@param1, avg, a, avg_a)"}};

std::list<std::list<std::string>> REDUCED_ONE_TAR_QUERIES_OUTER = {
    {"select(@param1, left_x, left_a)"},
    {"subset(@param1, left_x, 2, 4)"},
    {"where(@param1, left_a >= 2 and a <= 8)"},
    {"derive(@param1, derived_inner, left_x+1)"},
    {"aggregate(@param1, avg, left_a, avg_a)"}};

std::list<std::list<std::string>> TWO_TAR_QUERIES = {
    {"cross(@param1, @param2)"},
    {"dimjoin(@param1, @param2, x, x, y, y)",
     "dimjoin(@param1, @param2, x, x)",
     "dimjoin(@param1, @param2, x, y)"}};

std::list<std::string> NESTED_TWO_TAR_QUERIES = {
    "cross(cross(io, ep), et)",
    "dimjoin(cross(io, ep), io, left_x, x, left_y, y)",
    "dimjoin(dimjoin(io, io, x, x, y, y), io, left_x, x, left_y, y)"};

std::vector<std::string> TEST_QUERIES = {"select(select(io, x, y, a), x, a);",
                                         "select(select(ep, x, a), x, a);",
                                         "subset(select(it, x, y, a), x, 2, 4);",
                                         "subset(select(io, x, a), x, 2, 4);",
                                         "where(select(ep, x, y, a), a >= 2 and a <= 8);",
                                         "where(select(it, x, a), a >= 2 and a <= 8);",
                                         "derive(select(io, x, y, a), derived_outer, x+1);",
                                         "derive(select(ep, x, a), derived_outer, x+1);",
                                         "aggregate(select(it, x, y, a), avg, a, avg_a);",
                                         "aggregate(select(io, x, a), avg, a, avg_a);",
                                         "select(subset(ep, x, 2, 4), x, a);",
                                         "select(subset(it, x, 2, 4, y, 2, 2), x, a);",
                                         "select(subset(io, x, 1.2, 3.4, y, 0.732, 1.449), x, a);",
                                         "select(subset(ep, x, 1, 1, y, 2, 10), x, a);",
                                         "select(subset(it, x, 1, 2), x, a);",
                                         "select(subset(io, x, 3, 4), x, a);",
                                         "select(subset(ep, y, 2, 6), x, a);",
                                         "select(subset(it, x, 2, 10), x, a);",
                                         "subset(subset(io, x, 2, 4), x, 2, 4);",
                                         "subset(subset(ep, x, 2, 4, y, 2, 2), x, 2, 4);",
                                         "subset(subset(it, x, 1.2, 3.4, y, 0.732, 1.449), x, 2, 4);",
                                         "subset(subset(io, x, 1, 1, y, 2, 10), x, 2, 4);",
                                         "subset(subset(ep, x, 1, 2), x, 2, 4);",
                                         "subset(subset(it, x, 3, 4), x, 2, 4);",
                                         "subset(subset(io, y, 2, 6), x, 2, 4);",
                                         "subset(subset(ep, x, 2, 10), x, 2, 4);",
                                         "where(subset(it, x, 2, 4), a >= 2 and a <= 8);",
                                         "where(subset(io, x, 2, 4, y, 2, 2), a >= 2 and a <= 8);",
                                         "where(subset(ep, x, 1.2, 3.4, y, 0.732, 1.449), a >= 2 and a <= 8);",
                                         "where(subset(it, x, 1, 1, y, 2, 10), a >= 2 and a <= 8);",
                                         "where(subset(io, x, 1, 2), a >= 2 and a <= 8);",
                                         "where(subset(ep, x, 3, 4), a >= 2 and a <= 8);",
                                         "where(subset(it, y, 2, 6), a >= 2 and a <= 8);",
                                         "where(subset(io, x, 2, 10), a >= 2 and a <= 8);",
                                         "derive(subset(ep, x, 2, 4), derived_outer, x+1);",
                                         "derive(subset(it, x, 2, 4, y, 2, 2), derived_outer, x+1);",
                                         "derive(subset(io, x, 1.2, 3.4, y, 0.732, 1.449), derived_outer, x+1);",
                                         "derive(subset(ep, x, 1, 1, y, 2, 10), derived_outer, x+1);",
                                         "derive(subset(it, x, 1, 2), derived_outer, x+1);",
                                         "derive(subset(io, x, 3, 4), derived_outer, x+1);",
                                         "derive(subset(ep, y, 2, 6), derived_outer, x+1);",
                                         "derive(subset(it, x, 2, 10), derived_outer, x+1);",
                                         "aggregate(subset(io, x, 2, 4), avg, a, avg_a);",
                                         "aggregate(subset(ep, x, 2, 4, y, 2, 2), avg, a, avg_a);",
                                         "aggregate(subset(it, x, 1.2, 3.4, y, 0.732, 1.449), avg, a, avg_a);",
                                         "aggregate(subset(io, x, 1, 1, y, 2, 10), avg, a, avg_a);",
                                         "aggregate(subset(ep, x, 1, 2), avg, a, avg_a);",
                                         "aggregate(subset(it, x, 3, 4), avg, a, avg_a);",
                                         "aggregate(subset(io, y, 2, 6), avg, a, avg_a);",
                                         "aggregate(subset(ep, x, 2, 10), avg, a, avg_a);",
                                         "select(where(it, a >= 4 and a <= 8), x, a);",
                                         "select(where(io, x = 2 or x = 8), x, a);",
                                         "select(where(ep, not (a % 2) = 0), x, a);",
                                         "select(where(it, x = y), x, a);",
                                         "select(where(io, x < y), x, a);",
                                         "select(where(ep, x^2 < 16), x, a);",
                                         "subset(where(it, a >= 4 and a <= 8), x, 2, 4);",
                                         "subset(where(io, x = 2 or x = 8), x, 2, 4);",
                                         "subset(where(ep, not (a % 2) = 0), x, 2, 4);",
                                         "subset(where(it, x = y), x, 2, 4);",
                                         "subset(where(io, x < y), x, 2, 4);",
                                         "subset(where(ep, x^2 < 16), x, 2, 4);",
                                         "where(where(it, a >= 4 and a <= 8), a >= 2 and a <= 8);",
                                         "where(where(io, x = 2 or x = 8), a >= 2 and a <= 8);",
                                         "where(where(ep, not (a % 2) = 0), a >= 2 and a <= 8);",
                                         "where(where(it, x = y), a >= 2 and a <= 8);",
                                         "where(where(io, x < y), a >= 2 and a <= 8);",
                                         "where(where(ep, x^2 < 16), a >= 2 and a <= 8);",
                                         "derive(where(it, a >= 4 and a <= 8), derived_outer, x+1);",
                                         "derive(where(io, x = 2 or x = 8), derived_outer, x+1);",
                                         "derive(where(ep, not (a % 2) = 0), derived_outer, x+1);",
                                         "derive(where(it, x = y), derived_outer, x+1);",
                                         "derive(where(io, x < y), derived_outer, x+1);",
                                         "derive(where(ep, x^2 < 16), derived_outer, x+1);",
                                         "aggregate(where(it, a >= 4 and a <= 8), avg, a, avg_a);",
                                         "aggregate(where(io, x = 2 or x = 8), avg, a, avg_a);",
                                         "aggregate(where(ep, not (a % 2) = 0), avg, a, avg_a);",
                                         "aggregate(where(it, x = y), avg, a, avg_a);",
                                         "aggregate(where(io, x < y), avg, a, avg_a);",
                                         "aggregate(where(ep, x^2 < 16), avg, a, avg_a);",
                                         "select(derive(it, derived, x+1), x, a);",
                                         "select(derive(io, derived, a/1), x, a);",
                                         "select(derive(ep, derived, (x+a)*2), x, a);",
                                         "select(derive(it, derived, sqrt(x)+3), x, a);",
                                         "select(derive(io, derived, sin(a)*cos(a)), x, a);",
                                         "select(derive(ep, derived, floor(x)*a), x, a);",
                                         "subset(derive(it, derived, x+1), x, 2, 4);",
                                         "subset(derive(io, derived, a/1), x, 2, 4);",
                                         "subset(derive(ep, derived, (x+a)*2), x, 2, 4);",
                                         "subset(derive(it, derived, sqrt(x)+3), x, 2, 4);",
                                         "subset(derive(io, derived, sin(a)*cos(a)), x, 2, 4);",
                                         "subset(derive(ep, derived, floor(x)*a), x, 2, 4);",
                                         "where(derive(it, derived, x+1), a >= 2 and a <= 8);",
                                         "where(derive(io, derived, a/1), a >= 2 and a <= 8);",
                                         "where(derive(ep, derived, (x+a)*2), a >= 2 and a <= 8);",
                                         "where(derive(it, derived, sqrt(x)+3), a >= 2 and a <= 8);",
                                         "where(derive(io, derived, sin(a)*cos(a)), a >= 2 and a <= 8);",
                                         "where(derive(ep, derived, floor(x)*a), a >= 2 and a <= 8);",
                                         "derive(derive(it, derived, x+1), derived_outer, x+1);",
                                         "derive(derive(io, derived, a/1), derived_outer, x+1);",
                                         "derive(derive(ep, derived, (x+a)*2), derived_outer, x+1);",
                                         "derive(derive(it, derived, sqrt(x)+3), derived_outer, x+1);",
                                         "derive(derive(io, derived, sin(a)*cos(a)), derived_outer, x+1);",
                                         "derive(derive(ep, derived, floor(x)*a), derived_outer, x+1);",
                                         "aggregate(derive(it, derived, x+1), avg, a, avg_a);",
                                         "aggregate(derive(io, derived, a/1), avg, a, avg_a);",
                                         "aggregate(derive(ep, derived, (x+a)*2), avg, a, avg_a);",
                                         "aggregate(derive(it, derived, sqrt(x)+3), avg, a, avg_a);",
                                         "aggregate(derive(io, derived, sin(a)*cos(a)), avg, a, avg_a);",
                                         "aggregate(derive(ep, derived, floor(x)*a), avg, a, avg_a);",
                                         "select(aggregate(it, avg, a, avg_a), x, a);",
                                         "select(aggregate(io, count, a, c_a, x), x, a);",
                                         "select(aggregate(ep, max, a, max_a, x), x, a);",
                                         "select(aggregate(it, count, x, agg, x, y), x, a);",
                                         "select(aggregate(io, sum, y, sum_y, x), x, a);",
                                         "select(aggregate(ep, avg, y, avg_y, y), x, a);",
                                         "subset(aggregate(it, avg, a, avg_a), x, 2, 4);",
                                         "subset(aggregate(io, count, a, c_a, x), x, 2, 4);",
                                         "subset(aggregate(ep, max, a, max_a, x), x, 2, 4);",
                                         "subset(aggregate(it, count, x, agg, x, y), x, 2, 4);",
                                         "subset(aggregate(io, sum, y, sum_y, x), x, 2, 4);",
                                         "subset(aggregate(ep, avg, y, avg_y, y), x, 2, 4);",
                                         "where(aggregate(it, avg, a, avg_a), a >= 2 and a <= 8);",
                                         "where(aggregate(io, count, a, c_a, x), a >= 2 and a <= 8);",
                                         "where(aggregate(ep, max, a, max_a, x), a >= 2 and a <= 8);",
                                         "where(aggregate(it, count, x, agg, x, y), a >= 2 and a <= 8);",
                                         "where(aggregate(io, sum, y, sum_y, x), a >= 2 and a <= 8);",
                                         "where(aggregate(ep, avg, y, avg_y, y), a >= 2 and a <= 8);",
                                         "derive(aggregate(it, avg, a, avg_a), derived_outer, x+1);",
                                         "derive(aggregate(io, count, a, c_a, x), derived_outer, x+1);",
                                         "derive(aggregate(ep, max, a, max_a, x), derived_outer, x+1);",
                                         "derive(aggregate(it, count, x, agg, x, y), derived_outer, x+1);",
                                         "derive(aggregate(io, sum, y, sum_y, x), derived_outer, x+1);",
                                         "derive(aggregate(ep, avg, y, avg_y, y), derived_outer, x+1);",
                                         "aggregate(aggregate(it, avg, a, avg_a), avg, a, avg_a);",
                                         "aggregate(aggregate(io, count, a, c_a, x), avg, a, avg_a);",
                                         "aggregate(aggregate(ep, max, a, max_a, x), avg, a, avg_a);",
                                         "aggregate(aggregate(it, count, x, agg, x, y), avg, a, avg_a);",
                                         "aggregate(aggregate(io, sum, y, sum_y, x), avg, a, avg_a);",
                                         "aggregate(aggregate(ep, avg, y, avg_y, y), avg, a, avg_a);",
                                         "cross(select(io, x, a), it);",
                                         "dimjoin(select(io, x, a), io, x, x, y, y);",
                                         "dimjoin(select(io, x, a), ep, x, x);",
                                         "dimjoin(select(io, x, a), it, x, y);",
                                         "cross(subset(io, x, 2, 4), io);",
                                         "dimjoin(subset(io, x, 2, 4), ep, x, x, y, y);",
                                         "dimjoin(subset(io, x, 2, 4), it, x, x);",
                                         "dimjoin(subset(io, x, 2, 4), io, x, y);",
                                         "cross(where(io, a >= 2 and a <= 8), ep);",
                                         "dimjoin(where(io, a >= 2 and a <= 8), it, x, x, y, y);",
                                         "dimjoin(where(io, a >= 2 and a <= 8), io, x, x);",
                                         "dimjoin(where(io, a >= 2 and a <= 8), ep, x, y);",
                                         "cross(derive(io, derived_outer, x+1), it);",
                                         "dimjoin(derive(io, derived_outer, x+1), io, x, x, y, y);",
                                         "dimjoin(derive(io, derived_outer, x+1), ep, x, x);",
                                         "dimjoin(derive(io, derived_outer, x+1), it, x, y);",
                                         "cross(aggregate(io, avg, a, avg_a), io);",
                                         "dimjoin(aggregate(io, avg, a, avg_a), ep, x, x, y, y);",
                                         "dimjoin(aggregate(io, avg, a, avg_a), it, x, x);",
                                         "dimjoin(aggregate(io, avg, a, avg_a), io, x, y);",
                                         "select(cross(io, ep), left_x, left_a);",
                                         "subset(cross(io, it), left_x, 2, 4);",
                                         "where(cross(io, io), left_a >= 2 and a <= 8);",
                                         "derive(cross(io, ep), derived_inner, left_x+1);",
                                         "aggregate(cross(io, it), avg, left_a, avg_a);",
                                         "select(dimjoin(io, io, x, x, y, y), left_x, left_a);",
                                         "select(dimjoin(io, ep, x, x), left_x, left_a);",
                                         "select(dimjoin(io, it, x, y), left_x, left_a);",
                                         "subset(dimjoin(io, io, x, x, y, y), left_x, 2, 4);",
                                         "subset(dimjoin(io, ep, x, x), left_x, 2, 4);",
                                         "subset(dimjoin(io, it, x, y), left_x, 2, 4);",
                                         "where(dimjoin(io, io, x, x, y, y), left_a >= 2 and a <= 8);",
                                         "where(dimjoin(io, ep, x, x), left_a >= 2 and a <= 8);",
                                         "where(dimjoin(io, it, x, y), left_a >= 2 and a <= 8);",
                                         "derive(dimjoin(io, io, x, x, y, y), derived_inner, left_x+1);",
                                         "derive(dimjoin(io, ep, x, x), derived_inner, left_x+1);",
                                         "derive(dimjoin(io, it, x, y), derived_inner, left_x+1);",
                                         "aggregate(dimjoin(io, io, x, x, y, y), avg, left_a, avg_a);",
                                         "aggregate(dimjoin(io, ep, x, x), avg, left_a, avg_a);",
                                         "aggregate(dimjoin(io, it, x, y), avg, left_a, avg_a);",
                                         "cross(cross(io, ep), et)",
                                         "dimjoin(cross(io, ep), io, left_x, x, left_y, y)",
                                         "dimjoin(dimjoin(io, io, x, x, y, y), io, left_x, x, left_y, y)",
};

std::vector<std::string> TEST_QUERY_PLANS =
    {"TAR_1[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double] = SELECT (io, x, y, a)"
     "TAR_2[*]<ID:imp:int64(1,36/35/35,1)>[x:int32, a:double] = SELECT (TAR_1, x, a)",
     "TAR_1[*]<ID:imp:int64(1,25/24/24,1)>[x:float, a:double] = SELECT (ep, x, a)"
     "TAR_2[*]<ID:imp:int64(1,25/24/24,1)>[x:float, a:double] = SELECT (TAR_1, x, a)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double] = SELECT (it, x, y, a)"
     "TAR_2[*]<y:imp:int32(0,10/5/5,2), x:imp:int32(2,4/1/1,2)>[a:double] = SUBSET (TAR_1, x, 2.000000, 4.000000)",
     "",
     "TAR_1[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = SELECT (ep, x, y, a)"
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_1, a, 2.000000)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_1, a, 8.000000)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_1, and, TAR_2, TAR_3)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (TAR_1, TAR_4)",
     "TAR_1[*]<ID:imp:int64(1,36/35/35,1)>[x:int32, a:double] = SELECT (it, x, a)"
     "TAR_2[*]<ID:imp:int64(1,36/35/35,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_1, a, 2.000000)"
     "TAR_3[*]<ID:imp:int64(1,36/35/35,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_1, a, 8.000000)"
     "TAR_4[*]<ID:imp:int64(1,36/35/35,1)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_1, and, TAR_2, TAR_3)"
     "TAR_5[*]<ID:imp:int64(1,36/35/35,1)>[x:int32, a:double] = FILTER (TAR_1, TAR_4)",
     "TAR_1[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double] = SELECT (io, x, y, a)"
     "TAR_2[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, derived_outer:int32] = DERIVE (+, x, 1.000000, TAR_1, derived_outer)",
     "TAR_1[*]<ID:imp:int64(1,25/24/24,1)>[x:float, a:double] = SELECT (ep, x, a)"
     "TAR_2[*]<ID:imp:int64(1,25/24/24,1)>[x:float, a:double, derived_outer:float] = DERIVE (+, x, 1.000000, TAR_1, derived_outer)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double] = SELECT (it, x, y, a)"
     "TAR_2[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_1, avg, a, avg_a)",
     "TAR_1[*]<ID:imp:int64(1,36/35/35,1)>[x:int32, a:double] = SELECT (io, x, a)"
     "TAR_2[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_1, avg, a, avg_a)",
     "TAR_1[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, ep, x, 2.000000)"
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, ep, x, 4.000000)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_1, TAR_2)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_3)"
     "TAR_5[*]<ID:imp:int64(1,25/24/24,1)>[x:float, a:double] = SELECT (TAR_4, x, a)",
     "TAR_1[*]<y:imp:int32(2,2/0/0,2), x:imp:int32(2,4/1/1,2)>[a:double] = SUBSET (it, y, 2.000000, 2.000000, x, 2.000000, 4.000000)"
     "TAR_2[*]<ID:imp:int64(1,2/1/1,1)>[x:int32, a:double] = SELECT (TAR_1, x, a)",
     "TAR_1[footype]<y:imp:int32(1,1/0/0,1), x:imp:int32(2,3/1/1,1)>[a:double] = SUBSET (io, y, 0.732000, 1.449000, x, 1.200000, 3.400000)"
     "TAR_2[*]<ID:imp:int64(1,2/1/1,1)>[x:int32, a:double] = SELECT (TAR_1, x, a)",
     "TAR_1[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, ep, y, 2.000000)"
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, ep, y, 10.000000)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, ep, x, 1.000000)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, ep, x, 1.000000)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_1, TAR_2)"
     "TAR_6[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_5, TAR_3)"
     "TAR_7[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_6, TAR_4)"
     "TAR_8[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_7)"
     "TAR_9[*]<ID:imp:int64(1,25/24/24,1)>[x:float, a:double] = SELECT (TAR_8, x, a)",
     "TAR_1[*]<y:imp:int32(0,10/5/5,2), x:imp:int32(2,2/0/0,2)>[a:double] = SUBSET (it, x, 1.000000, 2.000000)"
     "TAR_2[*]<ID:imp:int64(1,6/5/5,1)>[x:int32, a:double] = SELECT (TAR_1, x, a)",
     "TAR_1[footype]<y:imp:int32(0,10/10/5,1), x:imp:int32(3,4/1/1,1)>[a:double] = SUBSET (io, x, 3.000000, 4.000000)"
     "TAR_2[*]<ID:imp:int64(1,12/11/11,1)>[x:int32, a:double] = SELECT (TAR_1, x, a)",
     "TAR_1[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, ep, y, 2.000000)"
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, ep, y, 6.000000)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_1, TAR_2)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_3)"
     "TAR_5[*]<ID:imp:int64(1,25/24/24,1)>[x:float, a:double] = SELECT (TAR_4, x, a)",
     "TAR_1[*]<y:imp:int32(0,10/5/5,2), x:imp:int32(2,10/4/4,2)>[a:double] = SUBSET (it, x, 2.000000, 10.000000)"
     "TAR_2[*]<ID:imp:int64(1,30/29/29,1)>[x:int32, a:double] = SELECT (TAR_1, x, a)",
     "TAR_1[footype]<y:imp:int32(0,10/10/5,1), x:imp:int32(2,4/2/2,1)>[a:double] = SUBSET (io, x, 2.000000, 4.000000)"
     "TAR_2[footype]<y:imp:int32(0,10/10/5,1), x:imp:int32(2,4/2/2,1)>[a:double] = SUBSET (TAR_1, x, 2.000000, 4.000000)",
     "TAR_1[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, ep, y, 2.000000)"
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, ep, y, 2.000000)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, ep, x, 2.000000)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, ep, x, 4.000000)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_1, TAR_2)"
     "TAR_6[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_5, TAR_3)"
     "TAR_7[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_6, TAR_4)"
     "TAR_8[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_7)"
     "TAR_9[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_8, x, 2.000000)"
     "TAR_10[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_8, x, 4.000000)"
     "TAR_11[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, TAR_8, TAR_9, TAR_10)"
     "TAR_12[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (TAR_8, TAR_11)",
     "",
     "",
     "TAR_1[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, ep, x, 1.000000)"
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, ep, x, 2.000000)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_1, TAR_2)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_3)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_4, x, 2.000000)"
     "TAR_6[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_4, x, 4.000000)"
     "TAR_7[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, TAR_4, TAR_5, TAR_6)"
     "TAR_8[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (TAR_4, TAR_7)",
     "",
     "TAR_1[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(2,6/4/4,1)>[a:double] = SUBSET (io, y, 2.000000, 6.000000)"
     "TAR_2[footype]<y:imp:int32(2,6/4/4,1), x:imp:int32(2,4/2/2,1)>[a:double] = SUBSET (TAR_1, x, 2.000000, 4.000000)",
     "TAR_1[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, ep, x, 2.000000)"
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, ep, x, 10.000000)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_1, TAR_2)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_3)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_4, x, 2.000000)"
     "TAR_6[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_4, x, 4.000000)"
     "TAR_7[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, TAR_4, TAR_5, TAR_6)"
     "TAR_8[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (TAR_4, TAR_7)",
     "TAR_1[*]<y:imp:int32(0,10/5/5,2), x:imp:int32(2,4/1/1,2)>[a:double] = SUBSET (it, x, 2.000000, 4.000000)"
     "TAR_2[*]<y:imp:int32(0,10/5/5,2), x:imp:int32(2,4/1/1,2)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_1, a, 2.000000)"
     "TAR_3[*]<y:imp:int32(0,10/5/5,2), x:imp:int32(2,4/1/1,2)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_1, a, 8.000000)"
     "TAR_4[*]<y:imp:int32(0,10/5/5,2), x:imp:int32(2,4/1/1,2)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_1, and, TAR_2, TAR_3)"
     "TAR_5[*]<y:imp:int32(0,10/5/5,2), x:imp:int32(2,4/1/1,2)>[a:double] = FILTER (TAR_1, TAR_4)",
     "TAR_1[footype]<y:imp:int32(2,2/0/0,1), x:imp:int32(2,4/2/2,1)>[a:double] = SUBSET (io, y, 2.000000, 2.000000, x, 2.000000, 4.000000)"
     "TAR_2[*]<y:imp:int32(2,2/0/0,1), x:imp:int32(2,4/2/2,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_1, a, 2.000000)"
     "TAR_3[*]<y:imp:int32(2,2/0/0,1), x:imp:int32(2,4/2/2,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_1, a, 8.000000)"
     "TAR_4[*]<y:imp:int32(2,2/0/0,1), x:imp:int32(2,4/2/2,1)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_1, and, TAR_2, TAR_3)"
     "TAR_5[footype]<y:imp:int32(2,2/0/0,1), x:imp:int32(2,4/2/2,1)>[a:double] = FILTER (TAR_1, TAR_4)",
     "TAR_1[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, ep, y, 0.732000)"
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, ep, y, 1.449000)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, ep, x, 1.200000)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, ep, x, 3.400000)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_1, TAR_2)"
     "TAR_6[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_5, TAR_3)"
     "TAR_7[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_6, TAR_4)"
     "TAR_8[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_7)"
     "TAR_9[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_8, a, 2.000000)"
     "TAR_10[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_8, a, 8.000000)"
     "TAR_11[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_8, and, TAR_9, TAR_10)"
     "TAR_12[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (TAR_8, TAR_11)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(2,10/4/4,2)>[a:double] = SUBSET (it, y, 2.000000, 10.000000, x, 1.000000, 1.000000, 1)"
     "TAR_2[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(2,10/4/4,2)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_1, a, 2.000000)"
     "TAR_3[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(2,10/4/4,2)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_1, a, 8.000000)"
     "TAR_4[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(2,10/4/4,2)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_1, and, TAR_2, TAR_3)"
     "TAR_5[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(2,10/4/4,2)>[a:double] = FILTER (TAR_1, TAR_4)",
     "TAR_1[footype]<y:imp:int32(0,10/10/5,1), x:imp:int32(1,2/1/1,1)>[a:double] = SUBSET (io, x, 1.000000, 2.000000)"
     "TAR_2[*]<y:imp:int32(0,10/10/5,1), x:imp:int32(1,2/1/1,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_1, a, 2.000000)"
     "TAR_3[*]<y:imp:int32(0,10/10/5,1), x:imp:int32(1,2/1/1,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_1, a, 8.000000)"
     "TAR_4[*]<y:imp:int32(0,10/10/5,1), x:imp:int32(1,2/1/1,1)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_1, and, TAR_2, TAR_3)"
     "TAR_5[footype]<y:imp:int32(0,10/10/5,1), x:imp:int32(1,2/1/1,1)>[a:double] = FILTER (TAR_1, TAR_4)",
     "TAR_1[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, ep, x, 3.000000)"
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, ep, x, 4.000000)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_1, TAR_2)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_3)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_4, a, 2.000000)"
     "TAR_6[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_4, a, 8.000000)"
     "TAR_7[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_4, and, TAR_5, TAR_6)"
     "TAR_8[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (TAR_4, TAR_7)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(2,6/2/2,2)>[a:double] = SUBSET (it, y, 2.000000, 6.000000)"
     "TAR_2[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(2,6/2/2,2)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_1, a, 2.000000)"
     "TAR_3[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(2,6/2/2,2)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_1, a, 8.000000)"
     "TAR_4[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(2,6/2/2,2)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_1, and, TAR_2, TAR_3)"
     "TAR_5[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(2,6/2/2,2)>[a:double] = FILTER (TAR_1, TAR_4)",
     "TAR_1[footype]<y:imp:int32(0,10/10/5,1), x:imp:int32(2,10/8/5,1)>[a:double] = SUBSET (io, x, 2.000000, 10.000000)"
     "TAR_2[*]<y:imp:int32(0,10/10/5,1), x:imp:int32(2,10/8/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_1, a, 2.000000)"
     "TAR_3[*]<y:imp:int32(0,10/10/5,1), x:imp:int32(2,10/8/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_1, a, 8.000000)"
     "TAR_4[*]<y:imp:int32(0,10/10/5,1), x:imp:int32(2,10/8/5,1)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_1, and, TAR_2, TAR_3)"
     "TAR_5[footype]<y:imp:int32(0,10/10/5,1), x:imp:int32(2,10/8/5,1)>[a:double] = FILTER (TAR_1, TAR_4)",
     "TAR_1[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, ep, x, 2.000000)"
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, ep, x, 4.000000)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_1, TAR_2)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_3)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, derived_outer:float] = DERIVE (+, x, 1.000000, TAR_4, derived_outer)",
     "TAR_1[*]<y:imp:int32(2,2/0/0,2), x:imp:int32(2,4/1/1,2)>[a:double] = SUBSET (it, y, 2.000000, 2.000000, x, 2.000000, 4.000000)"
     "TAR_2[*]<y:imp:int32(2,2/0/0,2), x:imp:int32(2,4/1/1,2)>[a:double, derived_outer:int32] = DERIVE (+, x, 1.000000, TAR_1, derived_outer)",
     "TAR_1[footype]<y:imp:int32(1,1/0/0,1), x:imp:int32(2,3/1/1,1)>[a:double] = SUBSET (io, y, 0.732000, 1.449000, x, 1.200000, 3.400000)"
     "TAR_2[footype]<y:imp:int32(1,1/0/0,1), x:imp:int32(2,3/1/1,1)>[a:double, derived_outer:int32] = DERIVE (+, x, 1.000000, TAR_1, derived_outer)",
     "TAR_1[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, ep, y, 2.000000)"
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, ep, y, 10.000000)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, ep, x, 1.000000)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, ep, x, 1.000000)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_1, TAR_2)"
     "TAR_6[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_5, TAR_3)"
     "TAR_7[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_6, TAR_4)"
     "TAR_8[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_7)"
     "TAR_9[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, derived_outer:float] = DERIVE (+, x, 1.000000, TAR_8, derived_outer)",
     "TAR_1[*]<y:imp:int32(0,10/5/5,2), x:imp:int32(2,2/0/0,2)>[a:double] = SUBSET (it, x, 1.000000, 2.000000)"
     "TAR_2[*]<y:imp:int32(0,10/5/5,2), x:imp:int32(2,2/0/0,2)>[a:double, derived_outer:int32] = DERIVE (+, x, 1.000000, TAR_1, derived_outer)",
     "TAR_1[footype]<y:imp:int32(0,10/10/5,1), x:imp:int32(3,4/1/1,1)>[a:double] = SUBSET (io, x, 3.000000, 4.000000)"
     "TAR_2[footype]<y:imp:int32(0,10/10/5,1), x:imp:int32(3,4/1/1,1)>[a:double, derived_outer:int32] = DERIVE (+, x, 1.000000, TAR_1, derived_outer)",
     "TAR_1[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, ep, y, 2.000000)"
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, ep, y, 6.000000)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_1, TAR_2)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_3)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, derived_outer:float] = DERIVE (+, x, 1.000000, TAR_4, derived_outer)",
     "TAR_1[*]<y:imp:int32(0,10/5/5,2), x:imp:int32(2,10/4/4,2)>[a:double] = SUBSET (it, x, 2.000000, 10.000000)"
     "TAR_2[*]<y:imp:int32(0,10/5/5,2), x:imp:int32(2,10/4/4,2)>[a:double, derived_outer:int32] = DERIVE (+, x, 1.000000, TAR_1, derived_outer)",
     "TAR_1[footype]<y:imp:int32(0,10/10/5,1), x:imp:int32(2,4/2/2,1)>[a:double] = SUBSET (io, x, 2.000000, 4.000000)"
     "TAR_2[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_1, avg, a, avg_a)",
     "TAR_1[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, ep, y, 2.000000)"
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, ep, y, 2.000000)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, ep, x, 2.000000)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, ep, x, 4.000000)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_1, TAR_2)"
     "TAR_6[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_5, TAR_3)"
     "TAR_7[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_6, TAR_4)"
     "TAR_8[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_7)"
     "TAR_9[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_8, avg, a, avg_a)",
     "TAR_1[*]<y:imp:int32(0,10/5/5,2), x:imp:int32(2,2/0/0,2)>[a:double] = SUBSET (it, y, 0.732000, 1.449000, x, 1.200000, 3.400000, 1)"
     "TAR_2[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_1, avg, a, avg_a)",
     "TAR_1[footype]<y:imp:int32(2,10/8/5,1), x:imp:int32(1,1/0/0,1)>[a:double] = SUBSET (io, y, 2.000000, 10.000000, x, 1.000000, 1.000000)"
     "TAR_2[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_1, avg, a, avg_a)",
     "TAR_1[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, ep, x, 1.000000)"
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, ep, x, 2.000000)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_1, TAR_2)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_3)"
     "TAR_5[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_4, avg, a, avg_a)",
     "TAR_1[*]<y:imp:int32(0,10/5/5,2), x:imp:int32(4,4/0/0,2)>[a:double] = SUBSET (it, x, 3.000000, 4.000000)"
     "TAR_2[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_1, avg, a, avg_a)",
     "TAR_1[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(2,6/4/4,1)>[a:double] = SUBSET (io, y, 2.000000, 6.000000)"
     "TAR_2[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_1, avg, a, avg_a)",
     "TAR_1[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, ep, x, 2.000000)"
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, ep, x, 10.000000)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, ep, TAR_1, TAR_2)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_3)"
     "TAR_5[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_4, avg, a, avg_a)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, it, a, 4.000000)"
     "TAR_2[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, it, a, 8.000000)"
     "TAR_3[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = LOGICAL (it, and, TAR_1, TAR_2)"
     "TAR_4[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double] = FILTER (it, TAR_3)"
     "TAR_5[*]<ID:imp:int64(1,36/35/35,1)>[x:int32, a:double] = SELECT (TAR_4, x, a)",
     "TAR_1[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (=, io, x, 2.000000)"
     "TAR_2[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (=, io, x, 8.000000)"
     "TAR_3[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = LOGICAL (io, or, TAR_1, TAR_2)"
     "TAR_4[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double] = FILTER (io, TAR_3)"
     "TAR_5[*]<ID:imp:int64(1,36/35/35,1)>[x:int32, a:double] = SELECT (TAR_4, x, a)",
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double] = DERIVE (%, a, 2.000000, ep, _aux1)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (=, TAR_2, _aux1, 0.000000)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (ep, not, TAR_3)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_4)"
     "TAR_6[*]<ID:imp:int64(1,25/24/24,1)>[x:float, a:double] = SELECT (TAR_5, x, a)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (=, it, x, y)"
     "TAR_2[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double] = FILTER (it, TAR_1)"
     "TAR_3[*]<ID:imp:int64(1,36/35/35,1)>[x:int32, a:double] = SELECT (TAR_2, x, a)",
     "TAR_1[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<, io, x, y)"
     "TAR_2[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double] = FILTER (io, TAR_1)"
     "TAR_3[*]<ID:imp:int64(1,36/35/35,1)>[x:int32, a:double] = SELECT (TAR_2, x, a)",
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double] = DERIVE (pow, x, 2.000000, ep, _aux1)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<, TAR_2, _aux1, 16.000000)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_3)"
     "TAR_5[*]<ID:imp:int64(1,25/24/24,1)>[x:float, a:double] = SELECT (TAR_4, x, a)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, it, a, 4.000000)"
     "TAR_2[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, it, a, 8.000000)"
     "TAR_3[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = LOGICAL (it, and, TAR_1, TAR_2)"
     "TAR_4[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double] = FILTER (it, TAR_3)"
     "TAR_5[*]<y:imp:int32(0,10/5/5,2), x:imp:int32(2,4/1/1,2)>[a:double] = SUBSET (TAR_4, x, 2.000000, 4.000000)",
     "TAR_1[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (=, io, x, 2.000000)"
     "TAR_2[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (=, io, x, 8.000000)"
     "TAR_3[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = LOGICAL (io, or, TAR_1, TAR_2)"
     "TAR_4[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double] = FILTER (io, TAR_3)"
     "TAR_5[footype]<y:imp:int32(0,10/10/5,1), x:imp:int32(2,4/2/2,1)>[a:double] = SUBSET (TAR_4, x, 2.000000, 4.000000)",
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double] = DERIVE (%, a, 2.000000, ep, _aux1)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (=, TAR_2, _aux1, 0.000000)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (ep, not, TAR_3)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_4)"
     "TAR_6[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_5, x, 2.000000)"
     "TAR_7[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_5, x, 4.000000)"
     "TAR_8[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, TAR_5, TAR_6, TAR_7)"
     "TAR_9[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (TAR_5, TAR_8)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (=, it, x, y)"
     "TAR_2[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double] = FILTER (it, TAR_1)"
     "TAR_3[*]<y:imp:int32(0,10/5/5,2), x:imp:int32(2,4/1/1,2)>[a:double] = SUBSET (TAR_2, x, 2.000000, 4.000000)",
     "TAR_1[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<, io, x, y)"
     "TAR_2[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double] = FILTER (io, TAR_1)"
     "TAR_3[footype]<y:imp:int32(0,10/10/5,1), x:imp:int32(2,4/2/2,1)>[a:double] = SUBSET (TAR_2, x, 2.000000, 4.000000)",
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double] = DERIVE (pow, x, 2.000000, ep, _aux1)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<, TAR_2, _aux1, 16.000000)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_3)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_4, x, 2.000000)"
     "TAR_6[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_4, x, 4.000000)"
     "TAR_7[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, TAR_4, TAR_5, TAR_6)"
     "TAR_8[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (TAR_4, TAR_7)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, it, a, 4.000000)"
     "TAR_2[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, it, a, 8.000000)"
     "TAR_3[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = LOGICAL (it, and, TAR_1, TAR_2)"
     "TAR_4[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double] = FILTER (it, TAR_3)"
     "TAR_5[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_4, a, 2.000000)"
     "TAR_6[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_4, a, 8.000000)"
     "TAR_7[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_4, and, TAR_5, TAR_6)"
     "TAR_8[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double] = FILTER (TAR_4, TAR_7)",
     "TAR_1[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (=, io, x, 2.000000)"
     "TAR_2[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (=, io, x, 8.000000)"
     "TAR_3[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = LOGICAL (io, or, TAR_1, TAR_2)"
     "TAR_4[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double] = FILTER (io, TAR_3)"
     "TAR_5[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_4, a, 2.000000)"
     "TAR_6[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_4, a, 8.000000)"
     "TAR_7[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_4, and, TAR_5, TAR_6)"
     "TAR_8[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double] = FILTER (TAR_4, TAR_7)",
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double] = DERIVE (%, a, 2.000000, ep, _aux1)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (=, TAR_2, _aux1, 0.000000)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (ep, not, TAR_3)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_4)"
     "TAR_6[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_5, a, 2.000000)"
     "TAR_7[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_5, a, 8.000000)"
     "TAR_8[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_5, and, TAR_6, TAR_7)"
     "TAR_9[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (TAR_5, TAR_8)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (=, it, x, y)"
     "TAR_2[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double] = FILTER (it, TAR_1)"
     "TAR_3[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_2, a, 2.000000)"
     "TAR_4[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_2, a, 8.000000)"
     "TAR_5[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_2, and, TAR_3, TAR_4)"
     "TAR_6[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double] = FILTER (TAR_2, TAR_5)",
     "TAR_1[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<, io, x, y)"
     "TAR_2[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double] = FILTER (io, TAR_1)"
     "TAR_3[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_2, a, 2.000000)"
     "TAR_4[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_2, a, 8.000000)"
     "TAR_5[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_2, and, TAR_3, TAR_4)"
     "TAR_6[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double] = FILTER (TAR_2, TAR_5)",
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double] = DERIVE (pow, x, 2.000000, ep, _aux1)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<, TAR_2, _aux1, 16.000000)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_3)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_4, a, 2.000000)"
     "TAR_6[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_4, a, 8.000000)"
     "TAR_7[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_4, and, TAR_5, TAR_6)"
     "TAR_8[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (TAR_4, TAR_7)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, it, a, 4.000000)"
     "TAR_2[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, it, a, 8.000000)"
     "TAR_3[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = LOGICAL (it, and, TAR_1, TAR_2)"
     "TAR_4[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double] = FILTER (it, TAR_3)"
     "TAR_5[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, derived_outer:int32] = DERIVE (+, x, 1.000000, TAR_4, derived_outer)",
     "TAR_1[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (=, io, x, 2.000000)"
     "TAR_2[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (=, io, x, 8.000000)"
     "TAR_3[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = LOGICAL (io, or, TAR_1, TAR_2)"
     "TAR_4[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double] = FILTER (io, TAR_3)"
     "TAR_5[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, derived_outer:int32] = DERIVE (+, x, 1.000000, TAR_4, derived_outer)",
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double] = DERIVE (%, a, 2.000000, ep, _aux1)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (=, TAR_2, _aux1, 0.000000)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (ep, not, TAR_3)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_4)"
     "TAR_6[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, derived_outer:float] = DERIVE (+, x, 1.000000, TAR_5, derived_outer)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (=, it, x, y)"
     "TAR_2[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double] = FILTER (it, TAR_1)"
     "TAR_3[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, derived_outer:int32] = DERIVE (+, x, 1.000000, TAR_2, derived_outer)",
     "TAR_1[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<, io, x, y)"
     "TAR_2[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double] = FILTER (io, TAR_1)"
     "TAR_3[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, derived_outer:int32] = DERIVE (+, x, 1.000000, TAR_2, derived_outer)",
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double] = DERIVE (pow, x, 2.000000, ep, _aux1)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<, TAR_2, _aux1, 16.000000)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_3)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, derived_outer:float] = DERIVE (+, x, 1.000000, TAR_4, derived_outer)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, it, a, 4.000000)"
     "TAR_2[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, it, a, 8.000000)"
     "TAR_3[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = LOGICAL (it, and, TAR_1, TAR_2)"
     "TAR_4[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double] = FILTER (it, TAR_3)"
     "TAR_5[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_4, avg, a, avg_a)",
     "TAR_1[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (=, io, x, 2.000000)"
     "TAR_2[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (=, io, x, 8.000000)"
     "TAR_3[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = LOGICAL (io, or, TAR_1, TAR_2)"
     "TAR_4[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double] = FILTER (io, TAR_3)"
     "TAR_5[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_4, avg, a, avg_a)",
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double] = DERIVE (%, a, 2.000000, ep, _aux1)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (=, TAR_2, _aux1, 0.000000)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (ep, not, TAR_3)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_4)"
     "TAR_6[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_5, avg, a, avg_a)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (=, it, x, y)"
     "TAR_2[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double] = FILTER (it, TAR_1)"
     "TAR_3[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_2, avg, a, avg_a)",
     "TAR_1[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<, io, x, y)"
     "TAR_2[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double] = FILTER (io, TAR_1)"
     "TAR_3[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_2, avg, a, avg_a)",
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double] = DERIVE (pow, x, 2.000000, ep, _aux1)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<, TAR_2, _aux1, 16.000000)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double] = FILTER (ep, TAR_3)"
     "TAR_5[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_4, avg, a, avg_a)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, derived:int32] = DERIVE (+, x, 1.000000, it, derived)"
     "TAR_2[*]<ID:imp:int64(1,36/35/35,1)>[x:int32, a:double] = SELECT (TAR_1, x, a)",
     "TAR_1[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, derived:double] = DERIVE (/, a, 1.000000, io, derived)"
     "TAR_2[*]<ID:imp:int64(1,36/35/35,1)>[x:int32, a:double] = SELECT (TAR_1, x, a)",
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double] = DERIVE (+, x, a, ep, _aux1)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double, derived:double] = DERIVE (*, _aux1, 2.000000, TAR_2, derived)"
     "TAR_4[*]<ID:imp:int64(1,25/24/24,1)>[x:float, a:double] = SELECT (TAR_3, x, a)",
     "TAR_2[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, _aux1:double] = DERIVE (sqrt, x, it, _aux1)"
     "TAR_3[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, _aux1:double, derived:double] = DERIVE (+, _aux1, 3.000000, TAR_2, derived)"
     "TAR_4[*]<ID:imp:int64(1,36/35/35,1)>[x:int32, a:double] = SELECT (TAR_3, x, a)",
     "TAR_2[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, _aux1:double] = DERIVE (sin, a, io, _aux1)"
     "TAR_4[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, _aux1:double, _aux3:double] = DERIVE (cos, a, TAR_2, _aux3)"
     "TAR_5[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, _aux1:double, _aux3:double, derived:double] = DERIVE (*, _aux1, _aux3, TAR_4, derived)"
     "TAR_6[*]<ID:imp:int64(1,36/35/35,1)>[x:int32, a:double] = SELECT (TAR_5, x, a)",
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double] = DERIVE (floor, x, ep, _aux1)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double, derived:double] = DERIVE (*, _aux1, a, TAR_2, derived)"
     "TAR_4[*]<ID:imp:int64(1,25/24/24,1)>[x:float, a:double] = SELECT (TAR_3, x, a)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, derived:int32] = DERIVE (+, x, 1.000000, it, derived)"
     "TAR_2[*]<y:imp:int32(0,10/5/5,2), x:imp:int32(2,4/1/1,2)>[a:double, derived:int32] = SUBSET (TAR_1, x, 2.000000, 4.000000)",
     "TAR_1[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, derived:double] = DERIVE (/, a, 1.000000, io, derived)"
     "TAR_2[footype]<y:imp:int32(0,10/10/5,1), x:imp:int32(2,4/2/2,1)>[a:double, derived:double] = SUBSET (TAR_1, x, 2.000000, 4.000000)",
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double] = DERIVE (+, x, a, ep, _aux1)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double, derived:double] = DERIVE (*, _aux1, 2.000000, TAR_2, derived)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_3, x, 2.000000)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_3, x, 4.000000)"
     "TAR_6[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, TAR_3, TAR_4, TAR_5)"
     "TAR_7[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double, derived:double] = FILTER (TAR_3, TAR_6)",
     "TAR_2[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, _aux1:double] = DERIVE (sqrt, x, it, _aux1)"
     "TAR_3[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, _aux1:double, derived:double] = DERIVE (+, _aux1, 3.000000, TAR_2, derived)"
     "TAR_4[*]<y:imp:int32(0,10/5/5,2), x:imp:int32(2,4/1/1,2)>[a:double, _aux1:double, derived:double] = SUBSET (TAR_3, x, 2.000000, 4.000000)",
     "TAR_2[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, _aux1:double] = DERIVE (sin, a, io, _aux1)"
     "TAR_4[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, _aux1:double, _aux3:double] = DERIVE (cos, a, TAR_2, _aux3)"
     "TAR_5[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, _aux1:double, _aux3:double, derived:double] = DERIVE (*, _aux1, _aux3, TAR_4, derived)"
     "TAR_6[footype]<y:imp:int32(0,10/10/5,1), x:imp:int32(2,4/2/2,1)>[a:double, _aux1:double, _aux3:double, derived:double] = SUBSET (TAR_5, x, 2.000000, 4.000000)",
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double] = DERIVE (floor, x, ep, _aux1)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double, derived:double] = DERIVE (*, _aux1, a, TAR_2, derived)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_3, x, 2.000000)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_3, x, 4.000000)"
     "TAR_6[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, TAR_3, TAR_4, TAR_5)"
     "TAR_7[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double, derived:double] = FILTER (TAR_3, TAR_6)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, derived:int32] = DERIVE (+, x, 1.000000, it, derived)"
     "TAR_2[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_1, a, 2.000000)"
     "TAR_3[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_1, a, 8.000000)"
     "TAR_4[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_1, and, TAR_2, TAR_3)"
     "TAR_5[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, derived:int32] = FILTER (TAR_1, TAR_4)",
     "TAR_1[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, derived:double] = DERIVE (/, a, 1.000000, io, derived)"
     "TAR_2[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_1, a, 2.000000)"
     "TAR_3[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_1, a, 8.000000)"
     "TAR_4[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_1, and, TAR_2, TAR_3)"
     "TAR_5[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, derived:double] = FILTER (TAR_1, TAR_4)",
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double] = DERIVE (+, x, a, ep, _aux1)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double, derived:double] = DERIVE (*, _aux1, 2.000000, TAR_2, derived)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_3, a, 2.000000)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_3, a, 8.000000)"
     "TAR_6[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_3, and, TAR_4, TAR_5)"
     "TAR_7[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double, derived:double] = FILTER (TAR_3, TAR_6)",
     "TAR_2[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, _aux1:double] = DERIVE (sqrt, x, it, _aux1)"
     "TAR_3[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, _aux1:double, derived:double] = DERIVE (+, _aux1, 3.000000, TAR_2, derived)"
     "TAR_4[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_3, a, 2.000000)"
     "TAR_5[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_3, a, 8.000000)"
     "TAR_6[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_3, and, TAR_4, TAR_5)"
     "TAR_7[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, _aux1:double, derived:double] = FILTER (TAR_3, TAR_6)",
     "TAR_2[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, _aux1:double] = DERIVE (sin, a, io, _aux1)"
     "TAR_4[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, _aux1:double, _aux3:double] = DERIVE (cos, a, TAR_2, _aux3)"
     "TAR_5[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, _aux1:double, _aux3:double, derived:double] = DERIVE (*, _aux1, _aux3, TAR_4, derived)"
     "TAR_6[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_5, a, 2.000000)"
     "TAR_7[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_5, a, 8.000000)"
     "TAR_8[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_5, and, TAR_6, TAR_7)"
     "TAR_9[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, _aux1:double, _aux3:double, derived:double] = FILTER (TAR_5, TAR_8)",
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double] = DERIVE (floor, x, ep, _aux1)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double, derived:double] = DERIVE (*, _aux1, a, TAR_2, derived)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_3, a, 2.000000)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_3, a, 8.000000)"
     "TAR_6[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (TAR_3, and, TAR_4, TAR_5)"
     "TAR_7[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double, derived:double] = FILTER (TAR_3, TAR_6)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, derived:int32] = DERIVE (+, x, 1.000000, it, derived)"
     "TAR_2[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, derived:int32, derived_outer:int32] = DERIVE (+, x, 1.000000, TAR_1, derived_outer)",
     "TAR_1[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, derived:double] = DERIVE (/, a, 1.000000, io, derived)"
     "TAR_2[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, derived:double, derived_outer:int32] = DERIVE (+, x, 1.000000, TAR_1, derived_outer)",
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double] = DERIVE (+, x, a, ep, _aux1)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double, derived:double] = DERIVE (*, _aux1, 2.000000, TAR_2, derived)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double, derived:double, derived_outer:float] = DERIVE (+, x, 1.000000, TAR_3, derived_outer)",
     "TAR_2[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, _aux1:double] = DERIVE (sqrt, x, it, _aux1)"
     "TAR_3[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, _aux1:double, derived:double] = DERIVE (+, _aux1, 3.000000, TAR_2, derived)"
     "TAR_4[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, _aux1:double, derived:double, derived_outer:int32] = DERIVE (+, x, 1.000000, TAR_3, derived_outer)",
     "TAR_2[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, _aux1:double] = DERIVE (sin, a, io, _aux1)"
     "TAR_4[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, _aux1:double, _aux3:double] = DERIVE (cos, a, TAR_2, _aux3)"
     "TAR_5[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, _aux1:double, _aux3:double, derived:double] = DERIVE (*, _aux1, _aux3, TAR_4, derived)"
     "TAR_6[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, _aux1:double, _aux3:double, derived:double, derived_outer:int32] = DERIVE (+, x, 1.000000, TAR_5, derived_outer)",
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double] = DERIVE (floor, x, ep, _aux1)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double, derived:double] = DERIVE (*, _aux1, a, TAR_2, derived)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double, derived:double, derived_outer:float] = DERIVE (+, x, 1.000000, TAR_3, derived_outer)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, derived:int32] = DERIVE (+, x, 1.000000, it, derived)"
     "TAR_2[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_1, avg, a, avg_a)",
     "TAR_1[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, derived:double] = DERIVE (/, a, 1.000000, io, derived)"
     "TAR_2[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_1, avg, a, avg_a)",
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double] = DERIVE (+, x, a, ep, _aux1)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double, derived:double] = DERIVE (*, _aux1, 2.000000, TAR_2, derived)"
     "TAR_4[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_3, avg, a, avg_a)",
     "TAR_2[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, _aux1:double] = DERIVE (sqrt, x, it, _aux1)"
     "TAR_3[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[a:double, _aux1:double, derived:double] = DERIVE (+, _aux1, 3.000000, TAR_2, derived)"
     "TAR_4[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_3, avg, a, avg_a)",
     "TAR_2[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, _aux1:double] = DERIVE (sin, a, io, _aux1)"
     "TAR_4[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, _aux1:double, _aux3:double] = DERIVE (cos, a, TAR_2, _aux3)"
     "TAR_5[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, _aux1:double, _aux3:double, derived:double] = DERIVE (*, _aux1, _aux3, TAR_4, derived)"
     "TAR_6[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_5, avg, a, avg_a)",
     "TAR_2[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double] = DERIVE (floor, x, ep, _aux1)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1), y:exp:float(0,4/4/4,1)>[a:double, _aux1:double, derived:double] = DERIVE (*, _aux1, a, TAR_2, derived)"
     "TAR_4[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_3, avg, a, avg_a)",
     "",
     "",
     "",
     "",
     "",
     "",
     "",
     "TAR_1[*]<x:imp:int32(0,10/10/5,1)>[c_a:double] = AGGREGATE (io, count, a, c_a, x)"
     "TAR_2[*]<x:imp:int32(2,4/2/2,1)>[c_a:double] = SUBSET (TAR_1, x, 2.000000, 4.000000)",
     "TAR_1[*]<x:exp:float(0,4/4/4,1)>[max_a:double] = AGGREGATE (ep, max, a, max_a, x)"
     "TAR_2[*]<x:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_1, x, 2.000000)"
     "TAR_3[*]<x:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_1, x, 4.000000)"
     "TAR_4[*]<x:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, TAR_1, TAR_2, TAR_3)"
     "TAR_5[*]<x:exp:float(0,4/4/4,1)>[max_a:double] = FILTER (TAR_1, TAR_4)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[agg:double] = AGGREGATE (it, count, x, agg, x, y)"
     "TAR_2[*]<y:imp:int32(0,10/5/5,2), x:imp:int32(2,4/1/1,2)>[agg:double] = SUBSET (TAR_1, x, 2.000000, 4.000000)",
     "TAR_1[*]<x:imp:int32(0,10/10/5,1)>[sum_y:double] = AGGREGATE (io, sum, y, sum_y, x)"
     "TAR_2[*]<x:imp:int32(2,4/2/2,1)>[sum_y:double] = SUBSET (TAR_1, x, 2.000000, 4.000000)",
     "",
     "",
     "",
     "",
     "",
     "",
     "",
     "",
     "TAR_1[*]<x:imp:int32(0,10/10/5,1)>[c_a:double] = AGGREGATE (io, count, a, c_a, x)"
     "TAR_2[*]<x:imp:int32(0,10/10/5,1)>[c_a:double, derived_outer:int32] = DERIVE (+, x, 1.000000, TAR_1, derived_outer)",
     "TAR_1[*]<x:exp:float(0,4/4/4,1)>[max_a:double] = AGGREGATE (ep, max, a, max_a, x)"
     "TAR_2[*]<x:exp:float(0,4/4/4,1)>[max_a:double, derived_outer:float] = DERIVE (+, x, 1.000000, TAR_1, derived_outer)",
     "TAR_1[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[agg:double] = AGGREGATE (it, count, x, agg, x, y)"
     "TAR_2[*]<x:imp:int32(0,10/5/5,2), y:imp:int32(0,10/5/5,2)>[agg:double, derived_outer:int32] = DERIVE (+, x, 1.000000, TAR_1, derived_outer)",
     "TAR_1[*]<x:imp:int32(0,10/10/5,1)>[sum_y:double] = AGGREGATE (io, sum, y, sum_y, x)"
     "TAR_2[*]<x:imp:int32(0,10/10/5,1)>[sum_y:double, derived_outer:int32] = DERIVE (+, x, 1.000000, TAR_1, derived_outer)",
     "",
     "",
     "",
     "",
     "",
     "",
     "",
     "TAR_1[*]<ID:imp:int64(1,36/35/35,1)>[x:int32, a:double] = SELECT (io, x, a)"
     "TAR_2[*]<right_x:imp:int32(0,10/5/5,2), right_y:imp:int32(0,10/5/5,2), left_ID:imp:int64(1,36/35/35,1)>[right_a:double, left_x:int32, left_a:double] = CROSS (TAR_1, it)",
     "",
     "",
     "",
     "TAR_1[footype]<y:imp:int32(0,10/10/5,1), x:imp:int32(2,4/2/2,1)>[a:double] = SUBSET (io, x, 2.000000, 4.000000)"
     "TAR_2[footype]<right_x:imp:int32(0,10/10/5,1), right_y:imp:int32(0,10/10/5,1), left_y:imp:int32(0,10/10/5,1), left_x:imp:int32(2,4/2/2,1)>[right_a:double, left_a:double] = CROSS (TAR_1, io)",
     "TAR_1[footype]<y:imp:int32(0,10/10/5,1), x:imp:int32(2,4/2/2,1)>[a:double] = SUBSET (io, x, 2.000000, 4.000000)"
     "TAR_2[footype]<left_y:imp:int32(0,0/0/0,1), left_x:imp:int32(0,0/0/0,1)>[right_a:double, left_a:double] = DIMJOIN (TAR_1, ep, x, x, y, y, 1, 1)",
     "TAR_1[footype]<y:imp:int32(0,10/10/5,1), x:imp:int32(2,4/2/2,1)>[a:double] = SUBSET (io, x, 2.000000, 4.000000)"
     "TAR_2[footype]<left_y:imp:int32(0,10/10/5,1), left_x:exp:int32(0,1/1/1,1), right_y:imp:int32(0,10/5/5,2)>[right_a:double, left_a:double] = DIMJOIN (TAR_1, it, x, x)",
     "TAR_1[footype]<y:imp:int32(0,10/10/5,1), x:imp:int32(2,4/2/2,1)>[a:double] = SUBSET (io, x, 2.000000, 4.000000)"
     "TAR_2[footype]<left_y:imp:int32(0,10/10/5,1), left_x:exp:int32(0,2/2/2,1), right_x:imp:int32(0,10/10/5,1)>[right_a:double, left_a:double] = DIMJOIN (TAR_1, io, x, y)",
     "TAR_1[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, io, a, 2.000000)"
     "TAR_2[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, io, a, 8.000000)"
     "TAR_3[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = LOGICAL (io, and, TAR_1, TAR_2)"
     "TAR_4[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double] = FILTER (io, TAR_3)"
     "TAR_5[footype]<right_x:exp:float(0,4/4/4,1), right_y:exp:float(0,4/4/4,1), left_x:imp:int32(0,10/10/5,1), left_y:imp:int32(0,10/10/5,1)>[right_a:double, left_a:double] = CROSS (TAR_4, ep)",
     "TAR_1[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, io, a, 2.000000)"
     "TAR_2[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, io, a, 8.000000)"
     "TAR_3[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = LOGICAL (io, and, TAR_1, TAR_2)"
     "TAR_4[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double] = FILTER (io, TAR_3)"
     "TAR_5[footype]<left_x:exp:int32(0,2/2/2,1), left_y:exp:int32(0,2/2/2,1)>[right_a:double, left_a:double] = DIMJOIN (TAR_4, it, x, x, y, y)",
     "TAR_1[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, io, a, 2.000000)"
     "TAR_2[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, io, a, 8.000000)"
     "TAR_3[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = LOGICAL (io, and, TAR_1, TAR_2)"
     "TAR_4[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double] = FILTER (io, TAR_3)"
     "TAR_5[footype]<left_x:exp:int32(0,5/5/5,1), left_y:imp:int32(0,10/10/5,1), right_y:imp:int32(0,10/10/5,1)>[right_a:double, left_a:double] = DIMJOIN (TAR_4, io, x, x)",
     "TAR_1[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, io, a, 2.000000)"
     "TAR_2[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, io, a, 8.000000)"
     "TAR_3[*]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[mask:subtar_position, offset:real_index] = LOGICAL (io, and, TAR_1, TAR_2)"
     "TAR_4[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double] = FILTER (io, TAR_3)"
     "TAR_5[footype]<left_x:imp:int32(0,0/0/0,1), left_y:imp:int32(0,10/10/5,1), right_x:exp:float(0,4/4/4,1)>[right_a:double, left_a:double] = DIMJOIN (TAR_4, ep, x, y, 1)",
     "TAR_1[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, derived_outer:int32] = DERIVE (+, x, 1.000000, io, derived_outer)"
     "TAR_2[footype]<right_x:imp:int32(0,10/5/5,2), right_y:imp:int32(0,10/5/5,2), left_x:imp:int32(0,10/10/5,1), left_y:imp:int32(0,10/10/5,1)>[right_a:double, left_a:double, left_derived_outer:int32] = CROSS (TAR_1, it)",
     "TAR_1[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, derived_outer:int32] = DERIVE (+, x, 1.000000, io, derived_outer)"
     "TAR_2[footype]<left_x:exp:int32(0,5/5/5,1), left_y:exp:int32(0,5/5/5,1)>[right_a:double, left_a:double, left_derived_outer:int32] = DIMJOIN (TAR_1, io, x, x, y, y)",
     "TAR_1[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, derived_outer:int32] = DERIVE (+, x, 1.000000, io, derived_outer)"
     "TAR_2[footype]<left_x:imp:int32(0,0/0/0,1), left_y:imp:int32(0,10/10/5,1), right_y:exp:float(0,4/4/4,1)>[right_a:double, left_a:double, left_derived_outer:int32] = DIMJOIN (TAR_1, ep, x, x, 1)",
     "TAR_1[footype]<x:imp:int32(0,10/10/5,1), y:imp:int32(0,10/10/5,1)>[a:double, derived_outer:int32] = DERIVE (+, x, 1.000000, io, derived_outer)"
     "TAR_2[footype]<left_x:exp:int32(0,2/2/2,1), left_y:imp:int32(0,10/10/5,1), right_x:imp:int32(0,10/5/5,2)>[right_a:double, left_a:double, left_derived_outer:int32] = DIMJOIN (TAR_1, it, x, y)",
     "TAR_1[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (io, avg, a, avg_a)"
     "TAR_2[*]<right_x:imp:int32(0,10/10/5,1), right_y:imp:int32(0,10/10/5,1), left_ID:imp:int32(0,0/0/0,1)>[right_a:double, left_avg_a:double] = CROSS (TAR_1, io)",
     "",
     "",
     "",
     "TAR_1[footype]<right_x:exp:float(0,4/4/4,1), right_y:exp:float(0,4/4/4,1), left_x:imp:int32(0,10/10/5,1), left_y:imp:int32(0,10/10/5,1)>[right_a:double, left_a:double] = CROSS (io, ep)"
     "TAR_2[*]<ID:imp:int64(1,900/899/899,1)>[left_x:int32, left_a:double] = SELECT (TAR_1, left_x, left_a)",
     "TAR_1[footype]<right_x:imp:int32(0,10/5/5,2), right_y:imp:int32(0,10/5/5,2), left_x:imp:int32(0,10/10/5,1), left_y:imp:int32(0,10/10/5,1)>[right_a:double, left_a:double] = CROSS (io, it)"
     "TAR_2[footype]<right_x:imp:int32(0,10/5/5,2), right_y:imp:int32(0,10/5/5,2), left_y:imp:int32(0,10/10/5,1), left_x:imp:int32(2,4/2/2,1)>[right_a:double, left_a:double] = SUBSET (TAR_1, left_x, 2.000000, 4.000000)",
     "",
     "TAR_1[footype]<right_x:exp:float(0,4/4/4,1), right_y:exp:float(0,4/4/4,1), left_x:imp:int32(0,10/10/5,1), left_y:imp:int32(0,10/10/5,1)>[right_a:double, left_a:double] = CROSS (io, ep)"
     "TAR_2[footype]<right_x:exp:float(0,4/4/4,1), right_y:exp:float(0,4/4/4,1), left_x:imp:int32(0,10/10/5,1), left_y:imp:int32(0,10/10/5,1)>[right_a:double, left_a:double, derived_inner:int32] = DERIVE (+, left_x, 1.000000, TAR_1, derived_inner)",
     "TAR_1[footype]<right_x:imp:int32(0,10/5/5,2), right_y:imp:int32(0,10/5/5,2), left_x:imp:int32(0,10/10/5,1), left_y:imp:int32(0,10/10/5,1)>[right_a:double, left_a:double] = CROSS (io, it)"
     "TAR_2[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_1, avg, left_a, avg_a)",
     "TAR_1[footype]<left_x:exp:int32(0,5/5/5,1), left_y:exp:int32(0,5/5/5,1)>[right_a:double, left_a:double] = DIMJOIN (io, io, x, x, y, y)"
     "TAR_2[*]<ID:imp:int64(1,36/35/35,1)>[left_x:int32, left_a:double] = SELECT (TAR_1, left_x, left_a)",
     "TAR_1[footype]<left_x:imp:int32(0,0/0/0,1), left_y:imp:int32(0,10/10/5,1), right_y:exp:float(0,4/4/4,1)>[right_a:double, left_a:double] = DIMJOIN (io, ep, x, x, 1)"
     "TAR_2[*]<ID:imp:int64(1,30/29/29,1)>[left_x:int32, left_a:double] = SELECT (TAR_1, left_x, left_a)",
     "TAR_1[footype]<left_x:exp:int32(0,2/2/2,1), left_y:imp:int32(0,10/10/5,1), right_x:imp:int32(0,10/5/5,2)>[right_a:double, left_a:double] = DIMJOIN (io, it, x, y)"
     "TAR_2[*]<ID:imp:int64(1,108/107/107,1)>[left_x:int32, left_a:double] = SELECT (TAR_1, left_x, left_a)",
     "TAR_1[footype]<left_x:exp:int32(0,5/5/5,1), left_y:exp:int32(0,5/5/5,1)>[right_a:double, left_a:double] = DIMJOIN (io, io, x, x, y, y)"
     "TAR_2[*]<left_x:exp:int32(0,5/5/5,1), left_y:exp:int32(0,5/5/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_1, left_x, 2.000000)"
     "TAR_3[*]<left_x:exp:int32(0,5/5/5,1), left_y:exp:int32(0,5/5/5,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_1, left_x, 4.000000)"
     "TAR_4[*]<left_x:exp:int32(0,5/5/5,1), left_y:exp:int32(0,5/5/5,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, TAR_1, TAR_2, TAR_3)"
     "TAR_5[footype]<left_x:exp:int32(0,5/5/5,1), left_y:exp:int32(0,5/5/5,1)>[right_a:double, left_a:double] = FILTER (TAR_1, TAR_4)",
     "TAR_1[footype]<left_x:imp:int32(0,0/0/0,1), left_y:imp:int32(0,10/10/5,1), right_y:exp:float(0,4/4/4,1)>[right_a:double, left_a:double] = DIMJOIN (io, ep, x, x, 1)"
     "TAR_2[*]<left_x:imp:int32(0,0/0/0,1), left_y:imp:int32(0,10/10/5,1), right_y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_1, left_x, 2.000000)"
     "TAR_3[*]<left_x:imp:int32(0,0/0/0,1), left_y:imp:int32(0,10/10/5,1), right_y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_1, left_x, 4.000000)"
     "TAR_4[*]<left_x:imp:int32(0,0/0/0,1), left_y:imp:int32(0,10/10/5,1), right_y:exp:float(0,4/4/4,1)>[mask:subtar_position, offset:real_index] = LOGICAL (and, TAR_1, TAR_2, TAR_3)"
     "TAR_5[footype]<left_x:imp:int32(0,0/0/0,1), left_y:imp:int32(0,10/10/5,1), right_y:exp:float(0,4/4/4,1)>[right_a:double, left_a:double] = FILTER (TAR_1, TAR_4)",
     "TAR_1[footype]<left_x:exp:int32(0,2/2/2,1), left_y:imp:int32(0,10/10/5,1), right_x:imp:int32(0,10/5/5,2)>[right_a:double, left_a:double] = DIMJOIN (io, it, x, y)"
     "TAR_2[*]<left_x:exp:int32(0,2/2/2,1), left_y:imp:int32(0,10/10/5,1), right_x:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (>=, TAR_1, left_x, 2.000000)"
     "TAR_3[*]<left_x:exp:int32(0,2/2/2,1), left_y:imp:int32(0,10/10/5,1), right_x:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = COMPARISON (<=, TAR_1, left_x, 4.000000)"
     "TAR_4[*]<left_x:exp:int32(0,2/2/2,1), left_y:imp:int32(0,10/10/5,1), right_x:imp:int32(0,10/5/5,2)>[mask:subtar_position, offset:real_index] = LOGICAL (and, TAR_1, TAR_2, TAR_3)"
     "TAR_5[footype]<left_x:exp:int32(0,2/2/2,1), left_y:imp:int32(0,10/10/5,1), right_x:imp:int32(0,10/5/5,2)>[right_a:double, left_a:double] = FILTER (TAR_1, TAR_4)",
     "",
     "",
     "",
     "TAR_1[footype]<left_x:exp:int32(0,5/5/5,1), left_y:exp:int32(0,5/5/5,1)>[right_a:double, left_a:double] = DIMJOIN (io, io, x, x, y, y)"
     "TAR_2[footype]<left_x:exp:int32(0,5/5/5,1), left_y:exp:int32(0,5/5/5,1)>[right_a:double, left_a:double, derived_inner:int32] = DERIVE (+, left_x, 1.000000, TAR_1, derived_inner)",
     "TAR_1[footype]<left_x:imp:int32(0,0/0/0,1), left_y:imp:int32(0,10/10/5,1), right_y:exp:float(0,4/4/4,1)>[right_a:double, left_a:double] = DIMJOIN (io, ep, x, x, 1)"
     "TAR_2[footype]<left_x:imp:int32(0,0/0/0,1), left_y:imp:int32(0,10/10/5,1), right_y:exp:float(0,4/4/4,1)>[right_a:double, left_a:double, derived_inner:int32] = DERIVE (+, left_x, 1.000000, TAR_1, derived_inner)",
     "TAR_1[footype]<left_x:exp:int32(0,2/2/2,1), left_y:imp:int32(0,10/10/5,1), right_x:imp:int32(0,10/5/5,2)>[right_a:double, left_a:double] = DIMJOIN (io, it, x, y)"
     "TAR_2[footype]<left_x:exp:int32(0,2/2/2,1), left_y:imp:int32(0,10/10/5,1), right_x:imp:int32(0,10/5/5,2)>[right_a:double, left_a:double, derived_inner:int32] = DERIVE (+, left_x, 1.000000, TAR_1, derived_inner)",
     "TAR_1[footype]<left_x:exp:int32(0,5/5/5,1), left_y:exp:int32(0,5/5/5,1)>[right_a:double, left_a:double] = DIMJOIN (io, io, x, x, y, y)"
     "TAR_2[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_1, avg, left_a, avg_a)",
     "TAR_1[footype]<left_x:imp:int32(0,0/0/0,1), left_y:imp:int32(0,10/10/5,1), right_y:exp:float(0,4/4/4,1)>[right_a:double, left_a:double] = DIMJOIN (io, ep, x, x, 1)"
     "TAR_2[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_1, avg, left_a, avg_a)",
     "TAR_1[footype]<left_x:exp:int32(0,2/2/2,1), left_y:imp:int32(0,10/10/5,1), right_x:imp:int32(0,10/5/5,2)>[right_a:double, left_a:double] = DIMJOIN (io, it, x, y)"
     "TAR_2[*]<ID:imp:int32(0,0/0/0,1)>[avg_a:double] = AGGREGATE (TAR_1, avg, left_a, avg_a)",
     "TAR_1[footype]<right_x:exp:float(0,4/4/4,1), right_y:exp:float(0,4/4/4,1), left_x:imp:int32(0,10/10/5,1), left_y:imp:int32(0,10/10/5,1)>[right_a:double, left_a:double] = CROSS (io, ep)"
     "TAR_2[footype]<right_x:exp:float(0,4/4/4,1), right_y:exp:float(0,4/4/4,1), left_right_x:exp:float(0,4/4/4,1), left_right_y:exp:float(0,4/4/4,1), left_left_x:imp:int32(0,10/10/5,1), left_left_y:imp:int32(0,10/10/5,1)>[right_a:double, left_right_a:double, left_left_a:double] = CROSS (TAR_1, et)",
     "TAR_1[footype]<right_x:exp:float(0,4/4/4,1), right_y:exp:float(0,4/4/4,1), left_x:imp:int32(0,10/10/5,1), left_y:imp:int32(0,10/10/5,1)>[right_a:double, left_a:double] = CROSS (io, ep)"
     "TAR_2[footype]<left_right_x:exp:float(0,4/4/4,1), left_right_y:exp:float(0,4/4/4,1), left_left_x:exp:int32(0,5/5/5,1), left_left_y:exp:int32(0,5/5/5,1)>[right_a:double, left_right_a:double, left_left_a:double] = DIMJOIN (TAR_1, io, left_x, x, left_y, y)",
     "TAR_1[footype]<left_x:exp:int32(0,5/5/5,1), left_y:exp:int32(0,5/5/5,1)>[right_a:double, left_a:double] = DIMJOIN (io, io, x, x, y, y)"
     "TAR_2[footype]<left_left_x:exp:int32(0,5/5/5,1), left_left_y:exp:int32(0,5/5/5,1)>[right_a:double, left_right_a:double, left_left_a:double] = DIMJOIN (TAR_1, io, left_x, x, left_y, y)",
    };

std::vector<std::string> TEST_ERROR_RESPONSES = {"",
                                                 "",
                                                 "",
                                                 "Schema element x is not a valid dimension.",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "Lower and upper bounds for dimension x must be between 2.000000 and 2.000000",
                                                 "Lower and upper bounds for dimension x must be between 1.000000 and 1.000000",
                                                 "",
                                                 "Lower and upper bounds for dimension x must be between 4.000000 and 4.000000",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "Schema element x is not a valid member.",
                                                 "Schema element a is not a valid member.",
                                                 "Schema element a is not a valid member.",
                                                 "Schema element a is not a valid member.",
                                                 "Schema element a is not a valid member.",
                                                 "Schema element x is not a valid member.",
                                                 "Schema element x is not a valid dimension.",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "Schema element x is not a valid dimension.",
                                                 "Data element a is not a valid member.",
                                                 "Data element a is not a valid member.",
                                                 "Data element a is not a valid member.",
                                                 "Data element a is not a valid member.",
                                                 "Data element a is not a valid member.",
                                                 "Data element a is not a valid member.",
                                                 "Data element x is not a valid member.",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "Data element x is not a valid member.",
                                                 "Schema element a is not a valid member.",
                                                 "Schema element a is not a valid member.",
                                                 "Schema element a is not a valid member.",
                                                 "Schema element a is not a valid member.",
                                                 "Schema element a is not a valid member.",
                                                 "Schema element a is not a valid member.",
                                                 "",
                                                 "Schema element x is not a dimension.",
                                                 "Schema element x is not a dimension.",
                                                 "Schema element x is not a dimension.",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "Schema element x is not a valid member.",
                                                 "Schema element x is not a valid member.",
                                                 "Schema element x is not a valid member.",
                                                 "",
                                                 "",
                                                 "Data element a is not a valid member.",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "Data element a is not a valid member.",
                                                 "Data element a is not a valid member.",
                                                 "Data element a is not a valid member.",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
                                                 "",
};

std::list<std::string> get_queries() {

  int64_t nameIndex = 0;
  std::list<std::string> queries;

  for (auto l1 : ONE_TAR_QUERIES) {
    for (auto l2 : REDUCED_ONE_TAR_QUERIES_INNER) {
      for (std::string q1 : l1) {
        for (std::string q2 : l2) {
          q2 += ";";
          replace(q1, "@param1", TAR_NAMES[nameIndex]);
          nameIndex = (nameIndex + 4) % TAR_NAMES.size();
          replace(q2, "@param1", q1);
          queries.push_back(q2);
        }
      }
    }
  }

  for (auto l1 : REDUCED_ONE_TAR_QUERIES_INNER) {
    for (auto l2 : TWO_TAR_QUERIES) {
      for (string q1 : l1) {
        for (string q2 : l2) {
          q2 += ";";
          replace(q1, "@param1", TAR_NAMES_PAIR[nameIndex][0]);
          replace(q2, "@param1", q1);
          replace(q2, "@param2", TAR_NAMES_PAIR[nameIndex][1]);
          nameIndex = (nameIndex + 4) % TAR_NAMES.size();
          queries.push_back(q2);
        }
      }
    }
  }

  for (auto l1 : TWO_TAR_QUERIES) {
    for (auto l2 : REDUCED_ONE_TAR_QUERIES_OUTER) {
      for (string q1 : l1) {
        for (string q2 : l2) {
          q2 += ";";
          replace(q1, "@param1", TAR_NAMES_PAIR[nameIndex][0]);
          replace(q1, "@param2", TAR_NAMES_PAIR[nameIndex][1]);
          replace(q2, "@param1", q1);
          nameIndex = (nameIndex + 4) % TAR_NAMES.size();
          queries.push_back(q2);
        }
      }
    }
  }

  for (auto q : NESTED_TWO_TAR_QUERIES) {
    queries.push_back(q);
  }

  return queries;
}

void parseQueries(QueryDataManagerPtr queryDataManager,
                  ParserPtr parser,
                  std::list<string> &queries,
                  std::list<string> &queryPlans,
                  std::list<string> &errorResponses) {

  for (auto query : queries) {
    queryDataManager->AddQueryTextPart(query);
    parser->Parse(queryDataManager);

    if (queryDataManager->GetQueryPlan() != nullptr) {
      queryPlans.push_back(queryDataManager->GetQueryPlan()->toString());
    } else {
      queryPlans.emplace_back("");
    }
    errorResponses.push_back(queryDataManager->GetErrorResponse());
    queryDataManager->Release();
  }
}

void get_parse_results() {
  auto builder = new MockModulesBuilder();
  auto parser = builder->BuildParser();
  auto queryDataManager = builder->BuildQueryDataManager();
  builder->RunBootQueries(SETUP_QUERIES);

  std::list<string> queryPlans;
  std::list<string> errorResponses;

  auto queries = get_queries();
  parseQueries(queryDataManager, parser, queries, queryPlans, errorResponses);

  std::cout << _LEFT_CURLY_BRACKETS;
  for (auto query : queries) {
    std::cout << _DOUBLE_QUOTE << query << _DOUBLE_QUOTE << _COMMA << _NEWLINE;
  }
  std::cout << _RIGHT_CURLY_BRACKETS << _NEWLINE << _NEWLINE;

  std::cout << _LEFT_CURLY_BRACKETS;
  for (auto queryPlan : queryPlans) {

    std::string quotedQueryPlan;
    if (queryPlan.empty()) {
      quotedQueryPlan +=
          std::string(_DOUBLE_QUOTE) + std::string(_DOUBLE_QUOTE);
    } else {
      std::istringstream iss(queryPlan);
      for (std::string line; std::getline(iss, line);) {
        quotedQueryPlan += _DOUBLE_QUOTE + line + _DOUBLE_QUOTE + _NEWLINE;
      }
    }
    std::cout << quotedQueryPlan << _COMMA << _NEWLINE;
  }
  std::cout << _RIGHT_CURLY_BRACKETS << _NEWLINE << _NEWLINE;

  std::cout << _LEFT_CURLY_BRACKETS;
  for (auto errorResponse : errorResponses) {
    std::cout << _DOUBLE_QUOTE << errorResponse << _DOUBLE_QUOTE << _COMMA
              << _NEWLINE;
  }
  std::cout << _RIGHT_CURLY_BRACKETS << _NEWLINE << _NEWLINE;
}