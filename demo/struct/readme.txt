官方包中的数据类型打包为字节数组的方法没有用缓存导致性能极度降低，因为localhost测试发包，cpu单核吃满，速度却只有1mb/s

解决办法: 加缓存参考文档 https://blog.csdn.net/chenchiheng/article/details/33023109