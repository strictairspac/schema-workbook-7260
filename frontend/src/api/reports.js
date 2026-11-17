import {api} from './client.js';
export const reportsApi={
  list:()=>api('/reports'),
  create:data=>api('/reports','POST',data),
  cancel:id=>api(`/reports/${id}/cancel`,'POST'),
  retry:id=>api(`/reports/${id}/retry`,'POST'),
  fileStatus:id=>api(`/reports/${id}/file`)
};
