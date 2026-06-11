import utils from './utils.mjs';

export default class KTable {
  /**
   * [constructor description]
   * @param  {[type]} maxsize [description]
   * @return {[type]}         [description]
   */
  constructor(maxsize = 10) {
    this.nid = utils.randomID();
    this.nodes = [];
    this.maxsize = maxsize;
  }
  /**
   * push node
   * @param  {[type]} node [description]
   * @return {[type]}      [description]
   */
  push(node) {
    if (this.nodes.length >= this.maxsize) {
      return;
    }
    this.nodes.push(node);
  }
}
