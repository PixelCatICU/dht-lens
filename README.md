## 介绍

dht-lens 是一个 DHT 爬虫 + BT Client 的结合体, 从全球 DHT 网络里"嗅探"人们正在下载的资源, 并把资源的`metadata`(种子的主要信息)从 远程 BT 客户端下载, 并生成资源磁力链接. 通过磁力链接, 你就可以下载到资源文件.

## 用途

你可以使用 dht-lens 打造私人种子库, 也拿它做资源数据挖掘与分析。

## 安装

```
npm install dht-lens
```

## 使用

```js
import dhtLens from 'dht-lens';

dhtLens((data) => {
  console.log(data); // 获取到的信息
});
```

建议放在公网上执行，最好是国外的 VPS。

## 待做

>* 效率优化
>* 数据保存
>* 跨平台 GUI 化
>* 数据共享
>* 资源下载
>* 视频流媒体播放
