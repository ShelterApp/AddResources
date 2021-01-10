![logo](https://github.com/ShelterApp/AddResources/blob/master/NewIconLogo_Purple.png)
``````
Shelter App Inc., is an all volunteer non-profit organization whose mission is to help homeless and low-income families connect to services using web and mobile app.
``````
# ShelterApp Architecture

We are trying to scrape data from different open data sets by removing duplicates and adding it into temp tables in MongoDB. These temp tables are populated in our Volunteer Portal where volunteers would manually validate the information before commiting it into our main collection in MongoDB from which our front end app pulls the data.
![architecture](https://github.com/ShelterApp/AddResources/blob/master/ShelterAppArchitecture.png)


## Getting Started: 

#### Shelter App currently has following types and sub-types of services. So, we are more interested in scraping this kind of information.
Type | Service Sub Type
-- | --
FOOD	| Food Pantry, Soup Kitchen
SHELTER	| Emergency Shelter, Transitional Housing 
HEALTH	| Medical Clinic, Mental Health Services, Substance Abuse Treatment
RESOURCES	| Miscellaneous Services, Support Services, Legal Assistance, Senior Resources
WORK	| Employment Assistance, Job Training

#### Scrapping guidelines and rules.
We have to have follow general scraping guilelines mentioned here for all our scrappers.
https://github.com/ShelterApp/AddResources/wiki#scraping-guidelines--standards


#### This repo is created to add resources by scraping from below open data sets
(https://drive.google.com/drive/folders/1V652EVRsCLHznKfHXIHylIREYbmsc3MB?usp=sharing)

1. IRS Links for Data Extraction: 
   - https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf
   - https://www.irs.gov/pub/irs-soi/eo_info.pdf
    
    *NTEE Codes of Interest:
    154 Health clinic
    166 Mental health care
    380 Low-income housing
    381 Low and moderate income housing
    566 Job training, counseling, or assistance
    B70 Libraries
    D40 Veterinary Services
    D99 Animal-Related N.E.C.
    E21 Community Health Systems
    E30 Health Treatment Facilities, Primarily Outpatient
    E32 Ambulatory Health Center, Community Clinic
    E50 Rehabilitative Medical Services
    E60 Health Support Services
    F32 Community Mental Health Center
    F40 Hot Line, Crisis Intervention Services
    F42 Rape Victim Services
    F60 Counseling, Support Groups
    F99 Mental Health, Crisis Intervention N.E.C.
    I80 Legal Services
    I83 Public Interest Law, Litigation
    J22 Vocational Training
    J99 Employment, Job Related N.E.C.
    K30 Food Service, Free Food Distribution Programs
    K31 Food Banks, Food Pantries
    K34 Congregate Meals
    K36 Meals on Wheels
    K99 Food, Agriculture, and Nutrition N.E.C.
    L30 Housing Search Assistance
    L40 Low-Cost Temporary Housing
    L41 Homeless, Temporary Shelter For
    L80 Housing Support Services -- Other
    L99 Housing, Shelter N.E.C.
    O20 Youth Centers, Clubs, Multipurpose
    O31 Big Brothers, Big Sisters
    O50 Youth Development Programs, Other
    O99 Youth Development N.E.C.
    P24 Salvation Army
    P26 Volunteers of America
    P30 Children's, Youth Services
    P40 Family Services
    P42 Single Parent Agencies, Services
    P43 Family Violence Shelters, Services
    P46 Family Counseling
    P51 Financial Counseling, Money Management
    P52 Transportation, Free or Subsidized
    P60 Emergency Assistance (Food, Clothing, Cash)
    P72 Half-Way House (Short-Term Residential Care)
    P81 Senior Centers, Services
    P82 Developmentally Disabled Centers, Services
    P84 Ethnic, Immigrant Centers, Services
    P85 Homeless Persons Centers, Services
    P86 Blind/Visually Impaired Centers, Services
    P87 Deaf/Hearing Impaired Centers, Services
    P99 Human Services - Multipurpose and Other N.E.C.
    
2. HUD Links:
   - https://www.hudexchange.info/resource/3031/pit-and-hic-data-since-2007/
   - https://hudgis-hud.opendata.arcgis.com/
    
3. CareerOneStop Link for Employment Assistance Resources:
   - https://www.careeronestop.org/Developers/Data/comprehensive-and-affiliate-american-job-centers.aspx
   
4. IMLS Dataset(Public Libraries in US)
     - https://www.imls.gov/research-evaluation/data-collection/public-libraries-survey
      
5. Washington State Resources form Northwest Hospitality:
     - https://www.nwhospitality.org/welcome-homeless-neighbor
     - https://airtable.com/shrkjUwUgqBU8vI1j/tblsz2z5rrnl9fdvG
     
6. California Food Resources:
     - https://controllerdata.lacity.org/dataset/Food-Resources-in-California/v2mg-qsxf
     
7.  Missouri Food Banks:
     - https://data.mo.gov/Social-Services/Food-Pantry-List/eb3y-vtsa
     
8.  Oregon Food Banks:
     - https://data.oregon.gov/Business/Filtered-Businesses-Food-Banks/nvp3-5wtz
     
9.  Pittsburgh Data(BigBurgh App):     
     - https://catalog.data.gov/dataset/bigburgh-social-service-listings
     
10. Washington DC Homeless Services & Shelters
     - https://opendata.dc.gov/datasets/47be87a68e7a4376a3bdbe15d85de398_6/data
     - https://opendata.dc.gov/datasets/87c5e68942304363a4578b30853f385d_25/data?geometry=-77.467%2C38.810%2C-76.540%2C38.997
     
11. Baltimore Homeless Shelters
     - https://data.baltimorecity.gov/Health/Homeless-Shelters/hyq3-8sxr

12. StreetLives NYC Open API(gogetta.nyc App), NYC Homeless Drop-In Centers, After School Programs: Runaway & Homeless Youth :
     - https://github.com/streetlives/streetlives-api
     - https://data.cityofnewyork.us/Social-Services/Directory-Of-Homeless-Drop-In-Centers/bmxf-3rd4 
     - https://data.cityofnewyork.us/Social-Services/DYCD-after-school-programs-Runaway-And-Homeless-Yo/ujsc-un6m ~~
          
13. Miami Open 211 (Need to call through API)
     - http://miami.open.211.developer.adopta.agency/

14. LittleHelpBook API/Data Links from Open Eugene
     - https://github.com/OpenEugene/little-help-book-data/blob/master/data/little-help-book.csv
     - https://github.com/OpenEugene/little-help-book-api
     - https://airtable.com/tblfr7CYabx9CwzO3/viwt6rHXp8T7o8DeJ
     - https://github.com/OpenEugene/little-help-book-web

15.  BayAreaCovidResources:
     - https://www.bayareacommunity.org/#/about-us
        
16. Canada Homeless Shelters:
     - https://open.canada.ca/data/en/dataset/7e0189e3-8595-4e62-a4e9-4fed6f265e10
    
17. BC Food Bank DataSet:
     - https://catalogue.data.gov.bc.ca/dataset/food-banks

18. OpenStreetMap Download Links:
   - https://planet.openstreetmap.org/
   - https://wiki.openstreetmap.org/wiki/Tag:amenity=social%20facility?uselang=en
