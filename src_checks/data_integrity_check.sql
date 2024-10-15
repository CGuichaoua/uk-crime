# Vérifier que les données communes aux deux tables son cohérentes
select * from outcomes o join street s on o.`Crime ID` = s.`Crime ID` 
where o.`Reported by` != s.`Reported by`  or o.`Falls within` != s.`Falls within`
or o.Longitude != s.Longitude or s.Latitude != o.Latitude 
or o.Location != s.Location or o.`LSOA code` != s.`LSOA code` 
or o.`LSOA name` != s.`LSOA name` 

# Chercher les doublons Crime ID dans outcomes
select `Crime ID`, count(*) as c
from outcomes
group by *
order by c desc

# Confirmer que les doublons Crime ID sont des doublons complets (version outcomes)
select count(distinct `Crime ID` , `Month` , `Reported by` , `Falls within` , Longitude , Latitude , Location , `LSOA code` , `LSOA name` , `Outcome type`) 
from outcomes o 
group by `Crime ID` 
having count(*) > 1
order by `Crime ID` 

# Chercher les doublons dans street
select `Crime ID`, count(*) as c
from street s 
group by `Crime ID` 
order by c desc

# Confirmer que les doublons Crime ID sont des doublons complets (version outcomes)
select count(distinct `Crime ID` , `Month` , `Reported by` , `Falls within` , Longitude , Latitude , Location , `LSOA code` , `LSOA name` , `Crime type`, `Last outcome category`) 
from street s 
where `Crime ID` != ""
group by `Crime ID` 
having count(*) > 1
order by `Crime ID` 

# Vérifier si il y a des disparités entre Reported by et Falls within
select *
from outcomes o 
where `Reported by` != `Falls within` 

select *
from street
where `Reported by` != `Falls within` 