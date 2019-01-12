#!/usr/bin/env bash

#----------------------------------------------------------------------
# File    : build_mongocxx.sh
# Author : Jeet Parmar <jeet@glabbr.com>
# Purpose : Building mongoc and mongocxx client
# Created : 12 Jan 2018 by Jeet Parmar <jeet@glabbr.com>
#
# Copyright (C) 2002-2019 Glabbr India Pvt. Ltd. All Rights Reserved.
#
# Licensed under the GNU GPL License, Version 3.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gnu.org/licenses/gpl-3.0.en.html
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#----------------------------------------------------------------------

DEPS_LOCATION=_build/deps

if [ -d "$DEPS_LOCATION" ]; then
    echo "cpp-driver fork already exist. delete $DEPS_LOCATION for a fresh checkout."
    exit 0
fi

OS=$(uname -s)
KERNEL=$(echo $(lsb_release -ds 2>/dev/null || cat /etc/*release 2>/dev/null | head -n1 | awk '{print $1;}') | awk '{print $1;}')

echo $OS
echo $KERNEL

C_DRIVER_REPO=https://github.com/mongodb/mongo-c-driver/releases/download/1.13.0/mongo-c-driver-1.13.0.tar.gz

CPP_DRIVER_REPO=https://github.com/mongodb/mongo-cxx-driver/archive/r3.4.0.zip
CPP_DRIVER_REV=$1

case $OS in
    Linux)
        case $KERNEL in
            CentOS)

                echo "Linux, CentOS"
                sudo yum -y install automake cmake gcc-c++ git libtool openssl-devel wget zip
            ;;

            Ubuntu)

                echo "Linux, Ubuntu"
                # check ubuntu version
                sudo apt-get -y update
                sudo apt-get -y install g++ make cmake libssl-dev
            ;;

            *) echo "Your system $KERNEL is not supported"
        esac
		export CFLAGS=-fPIC
		export CXXFLAGS=-fPIC
    ;;

    Darwin)
        brew install cmake openssl
        export OPENSSL_ROOT_DIR=$(brew --prefix openssl)
        export OPENSSL_INCLUDE_DIR=$OPENSSL_ROOT_DIR/include/
        export OPENSSL_LIBRARIES=$OPENSSL_ROOT_DIR/lib
        ;;

    *) echo "Your system $OS is not supported"
esac

mkdir -p $DEPS_LOCATION

#download mongoc and mongocxx

pushd $DEPS_LOCATION
wget  ${C_DRIVER_REPO}
pushd c-driver
wget  ${CPP_DRIVER_REPO}
pushd cpp-driver
popd
popd

#build

mkdir -p $DEPS_LOCATION/c-driver/cmake-build
pushd $DEPS_LOCATION/c-driver/cmake-build
cmake -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF ..
make -j 12
popd

mkdir -p $DEPS_LOCATION/cpp-driver/cmake-build
pushd $DEPS_LOCATION/cpp-driver/cmake-build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local/lib
make -j 12
popd