#!/bin/bash
#To download the source code 
git clone https://gitlab.kitware.com/paraview/paraview.git
cd paraview
git submodule update --init

#To update the code
git pull
git submodule update

#Configure build. Don't forget to set the following parameters: 
#PARAVIEW_USE_MPI=off, PARAVIEW_ENABLE_PYTHON=on and PARAVIEW_INSTALL_DEVELOPMENT_F=on.
cd ../
mkdir paraview_build
cd paraview_build
ccmake ../paraview

#Make with 4 cores
make -j 4
sudo make install
