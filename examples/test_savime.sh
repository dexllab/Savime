#!/bin/bash

#create datasets
savimec 'create_dataset("base:double", "@'$(pwd)'/base");'
#savimec 'create_dataset("dsexplict:float", "@'$(pwd)'/dsexplicit");'
savimec 'create_dataset("dsexplict:float", literal(1.5, 3.2, 4.7, 7.9, 13.1));'
savimec 'create_dataset("dspart1:int", "2:2:6:1");'
savimec 'create_dataset("dspart2:int", literal(2, 8));'
savimec 'create_dataset("dsepart1:long", literal(3, 1, 4));'
savimec 'create_dataset("dsepart2:long", literal(1, 2));'
savimec 'create_dataset("dstotalimplicitx:int", literal(2, 10, 4, 2, 6, 8));'
savimec 'create_dataset("dstotalimplicity:int", literal(2, 2, 4, 8, 8, 10));'
savimec 'create_dataset("dstotalexplicitx:long", literal(4, 2, 3, 1, 0));'
savimec 'create_dataset("dstotalexplicity:long", literal(0, 2, 3, 2, 4));'
savimec 'create_dataset("vec:int:2", literal(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));'
savimec 'create_dataset("vecstr:char:10", literal("apples", "oranges", "bananas", "melons", "pears", "peaches"));'

#create type
savimec 'create_type("footype(dim1, dim2, val)");'

#create tars
savimec 'create_tar("io", "footype", "implicit,x,int,0,10,1 | implicit,y,int,0,10,1", "a,double", "x, dim1, y, dim2, a, val");'
savimec 'create_tar("io2", "footype", "implicit,x,int,0,10,2 | implicit,y,int,0,10,2", "a,double", "x, dim1, y, dim2, a, val");'
savimec 'create_tar("iohalf", "footype", "implicit,x,float,0,10,0.5 | implicit,y,float,0,10,0.5", "a,double", "x, dim1, y, dim2, a, val");'
savimec 'create_tar("ip", "*", "implicit, x, int,0,10,2 | implicit, y, int,0,10,2", "a,double");'
savimec 'create_tar("it", "*", "implicit, x, int,0,10,2 | implicit, y, int,0,10,2", "a,double");'
savimec 'create_tar("eo", "*", "explicit, x, dsexplict  | explicit, y, dsexplict", "a,double");'
savimec 'create_tar("ep", "*", "explicit, x, dsexplict  | explicit, y, dsexplict", "a,double");'
savimec 'create_tar("et", "*", "explicit, x, dsexplict  | explicit, y, dsexplict", "a,double");'
savimec 'create_tar("vi", "*", "implicit, i, int, 1, 5, 1", "a,int:2");'
savimec 'create_tar("vs", "*", "implicit, i, int, 0, 5, 1", "s,char:10");'



#test load ORDERED SUBTARS INTO IMPLICIT DIMENSIONS (SIMPLEST CASE)
savimec 'load_subtar("io", "ordered, x, 0, 2  | ordered, y, 0, 2",  "a,base");'
savimec 'load_subtar("io", "ordered, x, 0, 2  | ordered, y, 3, 5", "a,base");'
savimec 'load_subtar("io", "ordered, x, 3, 5 | ordered, y, 0, 2",  "a,base");'
savimec 'load_subtar("io", "ordered, x, 3, 5 | ordered, y, 3, 5", "a,base");'
savimec 'load_subtar("io2", "ordered, x, #0, #4  | ordered, y, #0, #4",  "a,base");'
savimec 'load_subtar("io2", "ordered, x, #0, #4  | ordered, y, #6, #10", "a,base");'
savimec 'load_subtar("io2", "ordered, x, #6, #10 | ordered, y, #0, #4",  "a,base");'
savimec 'load_subtar("io2", "ordered, x, #6, #10 | ordered, y, #6, #10", "a,base");'
savimec 'load_subtar("iohalf", "ordered, x, 0, 2  | ordered, y, 0, 2",  "a,base");'
savimec 'load_subtar("iohalf", "ordered, x, 0, 2  | ordered, y, 3, 5", "a,base");'
savimec 'load_subtar("iohalf", "ordered, x, 3, 5 | ordered, y, 0, 2",  "a,base");'
savimec 'load_subtar("iohalf", "ordered, x, 3, 5 | ordered, y, 3, 5", "a,base");'

#test ORDERED SUBTARS INTO EXPLICIT DIMENSIONS
savimec 'load_subtar("eo", "ordered, x, #1.5, #4.7  | ordered, y, #1.5, #4.7", "a,base");'
savimec 'load_subtar("eo", "ordered, x, #1.5, #4.7  | ordered, y, #7.9, #13.1", "a,base");'
savimec 'load_subtar("eo", "ordered, x, #7.9, #13.1  | ordered, y, #1.5, #4.7", "a,base");'
savimec 'load_subtar("eo", "ordered, x, #7.9, #13.1  | ordered, y, #7.9, #13.1", "a,base");'

#test PARTIAL SUBTARS INTO IMPLICIT DIMENSIONS
savimec 'load_subtar("ip", "ordered, x, #0, #6 | partial, y, #0, #10, dspart1", "a,base");'
savimec 'load_subtar("ip", "ordered, x, #8, #10 | partial, y, #0, #10, dspart2", "a,base");'

#test PARTIAL SUBTARS INTO EXPLICIT DIMENSIONS
savimec 'load_subtar("ep", "ordered, x, #1.5, #4.7 | partial, y, #1.5, #13.1, dsepart1", "a,base");'
savimec 'load_subtar("ep", "ordered, x, #7.9, #13.1 | partial, y, #1.5, #13.1, dsepart2", "a,base");'

#test TOTAL SUBTARS INTO IMPLICIT DIMENSIONS
savimec 'load_subtar("it", "total, x, #0, #10, dstotalimplicitx | total, y, #0, #10, dstotalimplicity", "a,base");'

#test TOTAL SUBTARS INTO EXPLICIT DIMENSIONS
savimec 'load_subtar("et", "total, x, #1.5, #13.1, dstotalexplicitx | total, y, #1.5, #13.1, dstotalexplicity", "a,base");'

#test load vector TAR
savimec 'load_subtar("vi", "ordered, i, 0, 4", "a,vec");'
savimec 'load_subtar("vs", "ordered, i, 0, 5", "s,vecstr");'

#Queries
echo "Select Queries"
savimec 'select(io, x, y, a);'
savimec 'select(io2, x, y, a);'
savimec 'select(iohalf, x, y, a);'
savimec 'select(ip, x, y, a);'
savimec 'select(it, x, y, a);'
savimec 'select(eo, x, y, a);'
savimec 'select(ep, x, y, a);'
savimec 'select(et, x, y, a);'
savimec 'select(vi, i, a);'
savimec 'select(vs, i, s);'

echo "Subset Queries"
savimec 'subset(io, x, 2, 4);'
savimec 'subset(io2, x, 2, 4, y, 2, 2);'
savimec 'subset(iohalf, x, 1.2, 3.4, y, 0.732, 1.449);'
savimec 'subset(ip, x, 1, 1, y, 2, 10);'
savimec 'subset(it, x, 1, 2);'
savimec 'subset(eo, x, 3, 4);'
savimec 'subset(ep, y, 2, 6);'
savimec 'subset(et, x, 2, 10);'

echo "Where Queries"
savimec 'where(io, a >= 4 and a <= 8);'
savimec 'where(ip, x = 2 or x = 8);'
savimec 'where(it, not (a % 2) = 0);'
savimec 'where(eo, x = y);'
savimec 'where(ep, x < y);'
savimec 'where(et, x^2 < 16);'

echo "Derive Queries"
savimec 'derive(io, derived, x+1);'
savimec 'derive(ip, derived, a/1);'
savimec 'derive(it, derived, (x+a)*2);'
savimec 'derive(eo, derived, sqrt(x)+3);'
savimec 'derive(ep, derived, sin(a)*cos(a));'
savimec 'derive(et, derived, floor(x)*a);'

echo "Cross Queries"
savimec 'cross(io, et);'
savimec 'cross(ip, ep);'
savimec 'cross(it, eo);'
savimec 'cross(eo, eo);'
savimec 'cross(ep, it);'
savimec 'cross(et, eo);'

echo "Dimjoin Queries"
savimec 'dimjoin(io, io, x, x, y, y);'
savimec 'dimjoin(io, io, y, y);'
savimec 'dimjoin(io, et, x, x);'
savimec 'dimjoin(io, io2, x, x, y, y);'
savimec 'dimjoin(io2, iohalf, x, x, y, y);'
savimec 'dimjoin(ip, ep, y, y);'
savimec 'dimjoin(it, eo, x, x, y, y);'
savimec 'dimjoin(eo, eo, x, x, y, y);'
savimec 'dimjoin(ep, it, x, y);'
savimec 'dimjoin(et, eo, y, x);'

echo "Aggregate Queries"
savimec 'aggregate(io, avg, a, avg_a);'
savimec 'aggregate(iohalf, count, a, c_a, x);'
savimec 'aggregate(ip, max, a, max_a, x);'
savimec 'dimjoin(it, aggregate(it, min, a, min_a, y), y, y);'
savimec 'aggregate(eo, count, x, agg, x, y);'
savimec 'aggregate(ep, sum, y, sum_y, x);'
savimec 'aggregate(et, avg, y, avg_y, y);'

echo "Complex Queries Example: For every X, at what Y does 'a' reaches its peak?"
savimec 'aggregate(where(cross(io, aggregate(io, max, a, max_a, x)), left_a = right_max_a), max, left_y, y_at_max, left_x);'
#savimec 'where(cross(io, aggregate(io, max, a, max_a, x)), left_a = right_max_a);'
#savimec 'cross(io, aggregate(io, max, a, max_a, x));'

savimec 'aggregate(where(cross(et, aggregate(et, max, a, max_a, x)), left_a = right_max_a), max, left_y, y_at_max, left_x);'
