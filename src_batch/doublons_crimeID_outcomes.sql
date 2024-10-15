select `CrimeID`, count(CrimeID) as c
from outcomes
group by CrimeID
having c > 1
order by c desc