select distinct Policingoperation 
from stopandsearch_temp st 
= 0
-----------------------------------
select distinct Agerange 
from stopandsearch s  
=
25-34
10-17

over 34
18-24
under 10
-------------------------------------
select distinct Selfdefinedethnicity 
from stopandsearch_temp st 
=
White - Any other White background
White - English/Welsh/Scottish/Northern Irish/British

Other ethnic group - Not stated
Black/African/Caribbean/Black British - African
Black/African/Caribbean/Black British - Caribbean
Black/African/Caribbean/Black British - Any other Black/African/Caribbean background
Mixed/Multiple ethnic groups - White and Black Caribbean
Other ethnic group - Any other ethnic group
Mixed/Multiple ethnic groups - Any other Mixed/Multiple ethnic background
Asian/Asian British - Indian
Asian/Asian British - Pakistani
Asian/Asian British - Any other Asian background
Asian/Asian British - Chinese
White - Irish
Asian/Asian British - Bangladeshi
Mixed/Multiple ethnic groups - White and Asian
Mixed/Multiple ethnic groups - White and Black African
White - Gypsy or Irish Traveller
Other ethnic group - Arab
----------------------------------
select distinct Officerdefinedethnicity 
from stopandsearch_temp st 
=
White

Black
Mixed
Other
Asian
----------------------------------
select distinct Legislation 
from stopandsearch_temp st 
=
Misuse of Drugs Act 1971 (section 23)
Police and Criminal Evidence Act 1984 (section 1)

Firearms Act 1968 (section 47)
Criminal Justice and Public Order Act 1994 (section 60)
Poaching Prevention Act 1862 (section 2)
Criminal Justice Act 1988 (section 139B)
Wildlife and Countryside Act 1981 (section 19)
Psychoactive Substances Act 2016 (s36(2))
Customs and Excise Management Act 1979 (section 163)
Aviation Security Act 1982 (section 27(1))
Protection of Badgers Act 1992 (section 11)
Police and Criminal Evidence Act 1984 (section 6)
Deer Act 1991 (section 12)
Environmental Protection Act 1990 (section 34B )
Crossbows Act 1987 (section 4)
Public Stores Act 1875 (section 6)
Conservation of Seals Act 1970 (section 4)
Psychoactive Substances Act 2016 (s37(2))
Hunting Act 2004 (section 8)
Sporting Events Act 1985 (section 7)
----------------------------------
select distinct Objectofsearch 
from stopandsearch_temp st 
=
Controlled drugs
Stolen goods
Offensive weapons
Evidence of offences under the Act

Article for use in theft
Articles for use in criminal damage
Firearms
Fireworks
Anything to threaten or harm anyone
Goods on which duty has not been paid etc.
Game or poaching equipment
Evidence of wildlife offences
Psychoactive substances
Detailed object of search unavailable
Crossbows
Seals or hunting equipment
Evidence of hunting any wild mammal with a dog
-----------------------------------------
select distinct Outcome 
from stopandsearch_temp st
=
A no further action disposal
Arrest

Khat or Cannabis warning
Community resolution
Summons / charged by post
Caution (simple or conditional)
Penalty Notice for Disorder
--------------------------------------------
select distinct Crimetype 
from street_temp st 
=
Burglary
Criminal damage and arson
Public order
Violence and sexual offences
Anti-social behaviour
Other theft
Shoplifting
Bicycle theft
Vehicle crime
Drugs
Other crime
Robbery
Theft from the person
Possession of weapons
---------------------------------------
select distinct Lastoutcomecategory 
from street_temp st 
=
Status update unavailable
Investigation complete; no suspect identified
Unable to prosecute suspect

Court result unavailable
Offender given a drugs possession warning
Local resolution
Further action is not in the public interest
Formal action is not in the public interest
Further investigation is not in the public interest
Suspect charged as part of another case
Offender given a caution
Action to be taken by another organisation
Awaiting court outcome
Offender given penalty notice
Offender given community sentence
Under investigation
-------------------------------------------
dans street_and_search colonne 'context' vide





