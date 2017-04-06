# RocketMQ
从官方3.5.8 fork的分支。
1. 解决了3.5.8官方版本的已知所有issue。
2. DLQTopicDetector服务，原官方版本无法查看哪些消息进入了DLQ队列。
3. 修复了官方版未发现的writePerm失败问题。
4. 修复了 consumerOffset.json,topics.json等和其他配置文件保存不一致的问题。
5. 修复了broker端进行的flowControl，client端未同步而导致消息无法重发直接失败的问题。
6. 修复了broker在定时清理commitlog文件时，清理判断的一个错误。
7. MQ中添加了回声消息发送用以检查broker是否可用。

