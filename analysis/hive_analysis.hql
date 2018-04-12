
-- 建表 citylist
CREATE TABLE default.citylist
(prov string,
prov_code string,
prov_url string,
city string,
city_code string,
city_url string)
row format delimited fields terminated by '\t'
stored AS textfile;

-- 建表 city_weather
CREATE TABLE default.city_weather
(city_code string,
`month` string,
`date` string,
weather string,
tempreture string,
wind string)
row format delimited fields terminated by '\t'
stored AS textfile;

-- 导入数据
LOAD DATA LOCAL INPATH '/Users/warrenyoung/develops/chinese_weather/result/citylist.txt' INTO TABLE default.citylist;
LOAD DATA LOCAL INPATH '/Users/warrenyoung/develops/chinese_weather/result/weatherdetail.txt' INTO TABLE default.city_weather;

-- 数据整理
CREATE TABLE default.weather
(prov string,
prov_code string,
city string,
city_code string,
day string,
weather1 string,
weather2 string,
tempre1 string,
tempre2 string,
wind1 string,
wind2 string)
row format delimited fields terminated by '\t'
stored AS orcfile;

INSERT OVERWRITE TABLE default.weather
SELECT t1.prov, t1.prov_code, t1.city, t1.city_code, t2.`date`,
       split(t2.weather, '/')[0], split(t2.weather, '/')[1],
       CAST(substr(split(t2.tempreture, '/')[0], 0, LENGTH(split(t2.tempreture, '/')[0])-1) AS INT ),
       CAST(substr(split(t2.tempreture, '/')[1], 0, LENGTH(split(t2.tempreture, '/')[1])-1) AS INT ),
       split(t2.wind, '/')[0], split(t2.wind, '/')[1]
FROM default.citylist t1 JOIN default.city_weather t2 on t1.city_code=t2.city_code;

-- 计算各地平均气温， avg_weather.txt
SELECT prov, city, AVG(tempre1) temp1, AVG(tempre2) temp2
FROM default.weather
GROUP BY prov, city ORDER BY temp1, temp2;

-- 计算各地有雨天数、晴天天数，weathertype.txt
SELECT prov, city, COUNT(IF(weather1 LIKE '%晴%' AND weather2 like '%晴%', 1, NULL)) cnt1,
       COUNT(IF(weather1 LIKE '%雨%' OR weather2 like '%雨%', 1, NULL)) cnt2
FROM default.weather
GROUP BY prov, city ORDER BY cnt1, cnt2;
