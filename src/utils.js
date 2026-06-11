import crypto from 'crypto';
import { Buffer } from 'node:buffer'
import { Iconv } from 'iconv'

export default {
  /**
   * get random id
   * @return {[type]} [description]
   */
  randomID: () => {
    return crypto.createHash('sha1').update(crypto.randomBytes(20)).digest();
  },
  /**
   * decode nodes data
   * @param  {[type]} data [description]
   * @return {[type]}      [description]
   */
  decodeNodes(data){
    let nodes = [];
    for (let i = 0; i + 26 <= data.length; i += 26) {
      nodes.push({
        nid: data.slice(i, i + 20),
        address: `${data[i + 20]}.${data[i + 21]}.${data[i + 22]}.${data[i + 23]}`,
        port: data.readUInt16BE(i + 24)
      });
    }
    return nodes;
  },
  /**
   * get neighbor id
   * @param  {[type]} target [description]
   * @param  {[type]} nid    [description]
   * @return {[type]}        [description]
   */
  genNeighborID(target, nid){
    return Buffer.concat([target.slice(0, 10), nid.slice(10)]);
  },
  /**
   * to utf8 string
   * @param  {[type]} buffer [description]
   * @return {string|string}        [description]
   */
  toUtf8String(buffer){
    const langs = [
      'UTF-8', 'GBK', 'GB2312', 'BIG5', 'EUC-JP', 'EUC-KR', 'Shift_JIS',
      'UTF-16', 'UTF-16BE', 'UTF-16LE', 'UTF-EBCDIC',
      'HZ-GB-2312', 'ISCII', 'SJIS', 'EUC-CN', 'EUC-TW',
      'CP932', 'EUC-JP-MS', 'EUC-KR-MS',
      'ASCII', 'KOI8-R', 'KOI8-U', 'Windows-1251'
    ]
    let str = ''

    for(const c of langs) {
      try{
        const iconv = new Iconv(c, 'utf-8')
        str = iconv.convert(buffer).toString()
        if (c !== 'UTF-8') {
          console.log('----- success to decode ' + c)
        }
        break
      }catch(e){
        // console.log('----- failed to decode')
      }
    }
    return str ? str : Buffer.toString()

    // for(const encoding of langs) {
    //   try {
    //     str = iconv.decode(buffer, encoding)
    //     console.log('------- decode success ' + encoding)
    //     break
    //   } catch (e) {
    //     // console.log('failed to decode', buffer.toString('hex'))
    //   }
    // }
    return str
  }
};
