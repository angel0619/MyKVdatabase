
#!/bin/bash 
set -e # 遇到错误停止执行

# [代码随想录知识星球](https://www.programmercarl.com/other/kstar.html) 分布式存储项目脚本

# 更新软件源
sudo apt update

# 安装基本依赖工具
sudo apt-get install -y wget unzip make g++ build-essential

# 安装特定版本的 protoc
echo "安装特定版本的 protoc..."
sudo apt-get install protobuf-compiler libprotobuf-dev

# 验证安装 protoc
echo "验证 protoc 安装..."
sudo protoc --version

# 安装 Boost
echo "安装 Boost..."
sudo apt-get install -y libboost-dev libboost-test-dev libboost-all-dev

# 安装 CMake
CMAKE_TAR="cmake-3.26.4-linux-x86_64.tar.gz"

# 解压 CMake
echo "解压 CMake ${CMAKE_TAR}..."
sudo tar -zxvf ~/${CMAKE_TAR} -C /opt/

# 创建软链接到 /usr/local/bin
sudo ln -s /opt/cmake-3.26.4-linux-x86_64/bin/cmake /usr/local/bin/cmake

# 验证安装 CMake
echo "验证 CMake 安装..."
sudo cmake --version

# 解压 Muduo
MUDUO_ZIP="muduo-master.zip"

#echo "解压 Muduo ${MUDUO_ZIP}..."
unzip -o ~/${MUDUO_ZIP} -d ~/

# 进入 muduo 目录
cd ~/muduo-master

# 运行 Muduo 的 build.sh 脚本
echo "运行 build.sh 构建 Muduo..."
sudo ./build.sh

# 进行 muduo 库安装
echo "进行 muduo 库安装... ./build.sh install"
sudo ./build.sh install

# 将头文件和库文件移动到标准系统目录
echo "将头文件和库文件移动到标准目录..."

# 将头文件移动到 /usr/include
sudo mv ~/build/release-install-cpp11/include/muduo /usr/include/

# 将库文件移动到 /usr/local/lib
sudo mv ~/build/release-install-cpp11/lib/* /usr/local/lib/
#如果说这里移动的位置有问题你可以从home/你的用户名/build/release-install-cpp11/lib/

# 更新系统库路径
echo "更新系统库路径..."
sudo ldconfig

echo "Muduo 安装完成！"
