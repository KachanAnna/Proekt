import sqlalchemy as adb
import cx_Oracle as ora
from sqlalchemy import MetaData
import pandas as pd
l_user='Kachan_AN'
L_pass='cblytq_2050'

l_tns=ora.makedsn('13.95.167.129',1521,service_name='pdb1')
l_conn_ora=adb.create_engine(r'oracle://{p_user}:{p_pass}@{p_tns}'.format(
     p_user=l_user
     ,p_pass=L_pass
     ,p_tns=l_tns
     ))   
#print(l_conn_ora)

#вставка данных в таблицу rating
l_meta=MetaData(l_conn_ora) 
l_meta.reflect()
l_rating=l_meta.tables['rating']
l_file_excel=pd.read_csv(r'C:/Users/User/IMDb ratings.csv')
l_file_excel.replace({pd.NaT: None}, inplace=True)
l_list=l_file_excel.values.tolist()
for i in l_list:
    l_rating.insert([
l_rating.c.imdb_title_id
,l_rating.c.weighted_average_vote
,l_rating.c.total_votes
,l_rating.c.mean_vote
,l_rating.c.median_vote
,l_rating.c.votes_10
,l_rating.c.votes_9
,l_rating.c.votes_8
,l_rating.c.votes_7
,l_rating.c.votes_6
,l_rating.c.votes_5
,l_rating.c.votes_4
,l_rating.c.votes_3
,l_rating.c.votes_2
,l_rating.c.votes_1
,l_rating.c.allgenders_0age_avg_vote
,l_rating.c.allgenders_0age_votes
,l_rating.c.allgenders_18age_avg_vote
,l_rating.c.allgenders_18age_votes
,l_rating.c.allgenders_30age_avg_vote
,l_rating.c.allgenders_30age_votes
,l_rating.c.allgenders_45age_avg_vote
,l_rating.c.allgenders_45age_votes
,l_rating.c.males_allages_avg_vote
,l_rating.c.males_allages_votes
,l_rating.c.males_0age_avg_vote
,l_rating.c.males_0age_votes
,l_rating.c.males_18age_avg_vote
,l_rating.c.males_18age_votes
,l_rating.c.males_30age_avg_vote
,l_rating.c.males_30age_votes
,l_rating.c.males_45age_avg_vote
,l_rating.c.males_45age_votes
,l_rating.c.females_allages_avg_vote
,l_rating.c.females_allages_votes
,l_rating.c.females_0age_avg_vote
,l_rating.c.females_0age_votes
,l_rating.c.females_18age_avg_vote
,l_rating.c.females_18age_votes
,l_rating.c.females_30age_avg_vote
,l_rating.c.females_30age_votes
,l_rating.c.females_45age_avg_vote
,l_rating.c.females_45age_votes
,l_rating.c.top1000_voters_rating
,l_rating.c.top1000_voters_votes
,l_rating.c.us_voters_rating
,l_rating.c.us_voters_votes
,l_rating.c.non_us_voters_rating
,l_rating.c.non_us_voters_votes
]).values (
 imdb_title_id=i[0]
,weighted_average_vote=i[1]
,total_votes=i[2]
,mean_vote=i[3]
,median_vote=i[4]
,votes_10=i[5]
,votes_9=i[6]
,votes_8=i[7]
,votes_7=i[8]
,votes_6=i[9]
,votes_5=i[10]
,votes_4=i[11]
,votes_3=i[12]
,votes_2=i[13]
,votes_1=i[14]
,allgenders_0age_avg_vote=i[15]
,allgenders_0age_votes=i[16]
,allgenders_18age_avg_vote=i[17]
,allgenders_18age_votes=i[18]
,allgenders_30age_avg_vote=i[19]
,allgenders_30age_votes=i[20]
,allgenders_45age_avg_vote=i[21]
,allgenders_45age_votes=i[22]
,males_allages_avg_vote=i[23]
,males_allages_votes=i[24]
,males_0age_avg_vote=i[25]
,males_0age_votes=i[26]
,males_18age_avg_vote=i[27]
,males_18age_votes=i[28]
,males_30age_avg_vote=i[29]
,males_30age_votes=i[30]
,males_45age_avg_vote=i[31]
,males_45age_votes=i[32]
,females_allages_avg_vote=i[33]
,females_allages_votes=i[34]
,females_0age_avg_vote=i[35]
,females_0age_votes=i[36]
,females_18age_avg_vote=i[37]
,females_18age_votes=i[38]
,females_30age_avg_vote=i[39]
,females_30age_votes=i[40]
,females_45age_avg_vote=i[41]
,females_45age_votes=i[42]
,top1000_voters_rating=i[43]
,top1000_voters_votes=i[44]
,us_voters_rating=i[45]
,us_voters_votes=i[46]
,non_us_voters_rating=i[47]
,non_us_voters_votes=i[48]
                        
     ).execute()
print ('Данные в таблицу Rating вставлены')


#вставка данных в таблицу principals
l_principals=l_meta.tables['principals']
l_file_excel=pd.read_csv(r'C:/Users/User/IMDb title_principals.csv')
l_file_excel.replace({pd.NaT: None}, inplace=True)
l_list=l_file_excel.values.tolist()
for i in l_list:
    l_principals.insert([
l_principals.c.imdb_title_id
,l_principals.c.imdb_name_id
,l_principals.c.category
]).values (
    
imdb_title_id=i[0]
,imdb_name_id=i[1]
,category=i[2]

     ).execute()
print ('Данные в таблицу Principals вставлены')


#вставка данных в таблицу movies
l_movies=l_meta.tables['movies']
l_file_excel=pd.read_excel(r'C:/Users/User/IMDb movies.xlsx')
l_file_excel.replace({pd.NaT: None}, inplace=True)
l_list=l_file_excel.values.tolist()
for i in l_list:
    l_movies.insert([
    l_movies.c.imdb_title_id
    ,l_movies.c.title
    ,l_movies.c.original_title
    ,l_movies.c.date_published
    ,l_movies.c.genre
    ,l_movies.c.duration
    ,l_movies.c.country
    ,l_movies.c.actors
    ,l_movies.c.votes
    ,l_movies.c.reviews_from_users
    ,l_movies.c.reviews_from_critics
    ,l_movies.c.budget
    ,l_movies.c.usa_gross_income
    ,l_movies.c.worlwide_gross_income
    
    ]).values (
    imdb_title_id=i[0]
     ,title=i[1]
     ,original_title=i[2]
     ,date_published=i[3]
     ,genre=i[4]
     ,duration=i[5]
     ,country=i[6]
     ,actors=i[7]
     ,votes=i[8]
     ,reviews_from_users=i[9]
     ,reviews_from_critics=i[10]
     ,budget=i[11]
     ,usa_gross_income=i[12]
     ,worlwide_gross_income=i[13]
                        
     ).execute()
print ('Данные в таблицу Movies вставлены')  

#вставка данных в таблицу names   
l_names=l_meta.tables['names']
l_file_excel=pd.read_excel(r'C:/Users/User/IMDb names.xlsx')
l_file_excel.replace({pd.NaT: None}, inplace=True)
l_list=l_file_excel.values.tolist()
for i in l_list:
    l_names.insert([
l_names.c.imdb_name_id
,l_names.c.name
,l_names.c.birth_name 
,l_names.c.height 
,l_names.c.bio
,l_names.c.birth_details 
,l_names.c.date_of_birth
,l_names.c.place_of_birth
,l_names.c.death_details
,l_names.c.date_of_death
,l_names.c.place_of_death
,l_names.c.reason_of_death
,l_names.c.spouses_string
,l_names.c.spouses
,l_names.c.divorces
,l_names.c.spouses_with_children
,l_names.c.children
    
    ]).values (
     imdb_name_id=i[0]
     ,name=i[1]
     ,birth_name=i[2]
     ,height=i[3]
     ,bio=i[4]
     ,birth_details=i[5] 
     ,date_of_birth=i[6]
     ,place_of_birth=i[7]
     ,death_details=i[8]
     ,date_of_death=i[9]
     ,place_of_death=i[10]
     ,reason_of_death=i[11]
     ,spouses_string=i[12]
     ,spouses=i[13]
     ,divorces=i[14]
     ,spouses_with_children=i[15]
     ,children=i[16]
                        
     ).execute()

print ('Данные в таблицу Names вставлены')  

print ('Запуск процедуры обновления базовой таблицы')
l_conn_ora.execute(adb.text ('BEGIN KACHAN_AN_ORDERS_PACK.RATING_MOVIES_F;END;'))
print ('Базовая таблица обновлена')
print ('Запуск процедуры обновления итоговой таблицы')
l_conn_ora.execute(adb.text ('BEGIN KACHAN_AN_ORDERS_PACK.RATING_AGGR_F;END;'))
print ('Итоговая таблица обновлена')
    
    
    
    
    
    
    
    
    
    
    
    