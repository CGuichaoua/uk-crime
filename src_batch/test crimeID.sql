select * from outcomes as o join street as s on o.`CrimeID` = s.`CrimeID` 
where o.`Reportedby` != s.`Reportedby`  or o.`Fallswithin` != s.`Fallswithin`
or o.Longitude != s.Longitude or s.Latitude != o.Latitude 
or o.Location != s.Location or o.`LSOAcode` != s.`LSOAcode` 
or o.`LSOAname` != s.`LSOAname`
