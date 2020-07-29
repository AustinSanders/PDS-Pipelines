create view upc_geoserver_view as (
  select d.upcid, d.isisid, d.productid, d.source, d.detached_label, d.instrumentid, d.targetid, d.level, t.targetname, t.displayname, 
       i.instrument, j.jsonkeywords, s.processdate, s.starttime, s.solarlongitude, s.meangroundresolution, s.minimumemission, 
       s.maximumemission, s.emissionangle, s.minimumincidence, s.maximumincidence, s.incidenceangle, s.minimumphase, 
       s.maximumphase, s.phaseangle, s.err_flag, s.isisfootprint 
  from datafiles d 
  join targets t on (t.targetid=d.targetid) 
  join instruments i on (i.instrumentid=d.instrumentid) 
  join json_keywords j on (j.upcid=d.upcid) 
  join search_terms s on (s.upcid=d.upcid)
);