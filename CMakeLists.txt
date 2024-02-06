cmake_minimum_required(VERSION 3.0)
project(Mqtt-broker)

# Find Boost
find_package(Boost REQUIRED COMPONENTS system)

# Add your source files
set(SOURCE_FILES
    MqttClient.cpp
    MqttServer.cpp
    MqttParser.cpp
    UnitTest.cpp
    Main.cpp
)

# Add the executable
add_executable(Mqtt-broker ${SOURCE_FILES})

# Link against Boost
target_link_libraries(Mqtt-broker Boost::system)