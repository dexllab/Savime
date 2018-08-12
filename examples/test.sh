#!/bin/bash
./test_savime.sh > result.out
diff result.out expected.out
rm result.out
