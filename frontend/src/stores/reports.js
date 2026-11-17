import {reportsApi} from '../api/reports.js';
export const reportsStore={
  items:[],
  async refresh(){this.items=await reportsApi.list();return this.items;},
  async cancel(id){const item=await reportsApi.cancel(id);this.items=this.items.map(x=>x.id===id?item:x);return item;},
  async retry(id){const item=await reportsApi.retry(id);this.items=this.items.map(x=>x.id===id?item:x);return item;}
};
