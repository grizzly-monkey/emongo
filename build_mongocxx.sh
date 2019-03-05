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

INSTALL_LOC=$(pwd)
DEPS_LOCATION=_build/deps

if [ -d "$DEPS_LOCATION" ]; then
    rm -rf ${DEPS_LOCATION}
fi

OS=$(uname -s)
KERNEL=$(echo $(lsb_release -ds 2>/dev/null || cat /etc/*release 2>/dev/null | head -n1 | awk '{print $1;}') | awk '{print $1;}')
NUMBER_CORES=$(grep -c ^processor /proc/cpuinfo)

echo $OS
echo $KERNEL

C_DRIVER_REV=$1
CPP_DRIVER_REV=$2

C_DRIVER_REPO=https://github.com/mongodb/mongo-c-driver/archive/${C_DRIVER_REV}.zip
#1.13.0
CPP_DRIVER_REPO=https://github.com/mongodb/mongo-cxx-driver/archive/${CPP_DRIVER_REV}.zip
#r3.4.0

main()
{
	case ${OS} in
    	Linux)
        	case ${KERNEL} in
            	CentOS)
					make_centos
                ;;

            	Ubuntu)
					make_ubuntu
				;;

            	*) 
					echo "Your system $KERNEL is not supported"
					exit 1
        	esac
			export CFLAGS=-fPIC
			export CXXFLAGS=-fPIC
    	;;

    	Darwin)
        	make_darwin
        	;;

    	*) 
			echo "Your system $OS is not supported"
			exit 1
	esac
	mkdir -p ${DEPS_LOCATION}
	#makes new direcory inside current working directory
	mongoc_download
	mongoc_install
	mongocxx_download
	mongocxx_install
}

make_centos()
{
	echo "Linux, CentOS"
	sudo yum -y install automake cmake gcc-c++ git libtool openssl-devel wget zip
}

make_ubuntu()
{
	echo "Linux, Ubuntu"
	# check ubuntu version
	sudo apt-get -y update
	sudo apt-get -y install g++ make cmake libssl-dev wget zip
}

make_darwin()
{
	brew install cmake openssl
	export OPENSSL_ROOT_DIR=$(brew --prefix openssl)
	export OPENSSL_INCLUDE_DIR=${OPENSSL_ROOT_DIR}/include/
	export OPENSSL_LIBRARIES=${OPENSSL_ROOT_DIR}/lib
}

function fail_check
{
	#checks for error
    "$@"
    local status=$?
    if [ $status -ne 0 ]; then
        echo "error with $1" >&2
        exit 1
    fi
}

mongoc_download()
{
	pushd ${DEPS_LOCATION}
	
	#download monoc zip file
	fail_check wget  ${C_DRIVER_REPO}
}

mongoc_install()
{
	#install mongoc

	unzip $C_DRIVER_REV.zip

	pushd mongo-c-driver-${C_DRIVER_REV}
	fail_check cmake -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF -DCMAKE_INSTALL_PREFIX=${INSTALL_LOC}/priv
	fail_check make -j ${NUMBER_CORES}
	fail_check make install
	popd
}

mongocxx_download()
{
	
	#download monocxx zip file
	fail_check wget  ${CPP_DRIVER_REPO}
	
}

mongocxx_install()
{
	#install mongocxx

	unzip ${CPP_DRIVER_REV}.zip
	pushd mongo-cxx-driver-${CPP_DRIVER_REV}
	
	fail_check cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${INSTALL_LOC}/priv
	fail_check make -j ${NUMBER_CORES}
	fail_check make install
	popd
}


main

