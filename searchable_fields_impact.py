import findspark
findspark.init()

from pyspark.sql import SparkSession
import simplejson as json
import datetime
import pickle
from stemming.porter2 import stem
import re
from urllib.parse import unquote,unquote_plus
import csv
import pandas as pd


stopWordsSet = set(['a','and','an','at','are','as','be','if','for','by','but','into','is','in','not',
                     'of','it','no','on','such','that','or','the','their','then','there',
                     'these','they','this','to','was','will','with'])


def remove_first_last_sc(txt):
    
    FIRST_LAST_SC = re.compile(r'^[^A-Za-z0-9\s]+|[^A-Za-z0-9\s]+$', re.IGNORECASE)
    
    txt=str(txt).lower().strip()
    check_list = FIRST_LAST_SC.findall(txt)
    for item in check_list:
        try:
            txt=txt.replace(item,'')
        except Exception as e:
            print(e)
    return(txt.strip())
    

def stems(txt):
    txt=str(txt).lower().strip()
    stemmed_txt = ' '.join([stem(word) for word in txt.split()])
    return(stemmed_txt)

def remove_char_encoding(txt):
    txt=str(txt)
    return(unquote(unquote_plus(txt)))

def product_dict_preprocessing(product_dict):
    product_dict = json.loads(product_dict)
    product_id  = product_dict['uniqueId']
    product_dict_items = product_dict.items()
    for item in product_dict_items:
        yield((item,product_id))
        
def build_unigram_field_product_map(row):
    unigram = row[0]
    field_product_map_list = row[1]
    consolidated_dict = {}
    for field_product_map in field_product_map_list:
        field = str(field_product_map[0])
        products = field_product_map[1]
        consolidated_dict[field]=products
    return((unigram,consolidated_dict))

def split_to_unigrams(row):
    
    field = row[0]
    value = row[1]
    unigrams = []
    if(isinstance(value,str)):
        unigrams = value.split()
    elif(isinstance(value, list)):
        for element in value:
            unigrams+=str(element).split()
    elif(isinstance(value,(bool,int,float))):
        unigrams.append(str(value))
    
    for unigram in unigrams:
        yield((field,stems(remove_first_last_sc(unigram))))

def split_to_unigrams_v2(row):
    field = row[0][0]
    value = row[0][1]
    uniqueId = row[1]
    unigrams = []
    if(isinstance(value,str)):
        unigrams = value.split()
    elif(isinstance(value, list)):
        for element in value:
            unigrams+=str(element).split()
    elif(isinstance(value,(bool,int,float))):
        unigrams.append(str(value))
    
    for unigram in unigrams:
        yield((unigram,field),[uniqueId])
        
def filter_fields(row):
    unigram=row[0]
    consolidated_dict=row[1]
    for field in delete_fields:
        if field in consolidated_dict:
            del consolidated_dict[field]
    return((unigram,consolidated_dict))


def build_field_productUnigram_map(row):
    unigram = row[0]
    consolidated_dict=row[1]
    for field,product_set in consolidated_dict.items():
        productUnigram_map = set()
        for product in product_set:
            productUnigram=product+">>"+stems(remove_first_last_sc(unigram))
            productUnigram_map.add(productUnigram)
        yield((field, productUnigram_map))


def find_fields_byUnigram(row):
    unigram=stems(remove_first_last_sc(row[0]))
    consolidated_dict = row[1]
#    output_dict = {}
    for i,(field,product_set) in enumerate(sorted(consolidated_dict.items(), key=lambda x:len(x[1]), reverse=True)):
        field_rank=i+1
        if(i==0):
#            output_dict[unigram] = []
#            top_field=field
            top_product_set=product_set
            top_field_flag=1
            field_flag=1
            all_products=unique_products=product_set
            intersect_products=set()
            unigram_count_covered=1
            unigram_count_mandatory=1
            
            
            field_properties={'field_rank':field_rank,
                       'top_field_flag':top_field_flag,
                       'field_flag':field_flag,
                       'all_products':all_products,
                       'unique_products':unique_products,
                       'intersect_products':intersect_products,
                       'unigram_count_covered':unigram_count_covered,
                       'unigram_count_mandatory':unigram_count_mandatory}
            
            yield((field,field_properties))
            continue
        
        if(product_set.issubset(top_product_set)):
            top_field_flag=0
            field_flag=0
            all_products=intersect_products=product_set
            unique_products=set()
            unigram_count_covered=1
            unigram_count_mandatory=0
            
        else:
            top_field_flag=0
            field_flag=1
            all_products = product_set
            unique_products=product_set.difference(top_product_set)
            intersect_products=product_set.intersection(top_product_set)
            unigram_count_covered=1
            unigram_count_mandatory=1
            
            top_product_set = top_product_set.union(product_set) #updating top product set with products from new field
            
            
        field_properties={'field_rank':field_rank,
                   'top_field_flag':top_field_flag,
                   'field_flag':field_flag,
                   'all_products':all_products,
                   'unique_products':unique_products,
                   'intersect_products':intersect_products,
                   'unigram_count_covered':unigram_count_covered,
                   'unigram_count_mandatory':unigram_count_mandatory}
        yield((field,field_properties))
    

def build_field_aggregations(row):
    field=row[0]
    field_properties_list=row[1]
    field_properties_agg={'field_rank_sum':0,
                          'top_field_flag':0,
                          'field_flag':0,
                          'all_products':set(),
                          'unique_products':set(),
                          'intersect_products':set(),
                          'unigram_count_covered':0,
                          'unigram_count_mandatory':0
                          }
    for field_property_set in field_properties_list:
        field_properties_agg['field_rank_sum']+=field_property_set['field_rank']
        field_properties_agg['top_field_flag']+=field_property_set['top_field_flag']
        field_properties_agg['field_flag']+=field_property_set['field_flag']
        field_properties_agg['all_products']=field_properties_agg['all_products'].union(field_property_set['all_products'])
        field_properties_agg['unique_products']=field_properties_agg['unique_products'].union(field_property_set['unique_products'])
        field_properties_agg['intersect_products']=field_properties_agg['intersect_products'].union(field_property_set['intersect_products'])
        field_properties_agg['unigram_count_covered']+=field_property_set['unigram_count_covered']
        field_properties_agg['unigram_count_mandatory']+=field_property_set['unigram_count_mandatory']
    field_properties_agg['field_rank_avg']=field_properties_agg['field_rank_sum']/len(field_properties_list)
    
    return([field,field_properties_agg['field_rank_avg'],field_properties_agg['top_field_flag'],field_properties_agg['field_flag'],
            len(field_properties_agg['all_products']),len(field_properties_agg['unique_products']),len(field_properties_agg['intersect_products']),
            field_properties_agg['unigram_count_covered'],field_properties_agg['unigram_count_mandatory']])
    

current_relevance_config = open("","r",encoding="utf8")  # CSV file which contains the current relevancy configuration
reader = csv.reader(current_relevance_config)
relevance_config_dict = {}
for i,row in enumerate(reader):
    if(i==0):
        continue
    field = row[0]
    weight = row[1]
    relevance_config_dict[field]=weight


start_time = datetime.datetime.now()

raw_query_filepath = ""  #Input search term dump csv filepath
stemmed_unstemmed_query_map = {}
query_queryUnigram_map = []
query_unigram_set = set()

with open(raw_query_filepath, 'r', encoding='utf8', errors='ignore') as raw_query_file:
    reader = csv.reader(raw_query_file)
    for i,row in enumerate(reader):
        if(i==0):
            continue
        query = row[0]
        relevant_stemmed_query = stems(remove_first_last_sc(remove_char_encoding(query))) #re.sub(r'[\s]+', ' ', ' '.join([stems(remove_first_last_sc(token)) for token in remove_first_last_sc(remove_char_encoding(query)).split()]).strip()) ##-- a stricter method to form stemmed queries- use if expected raw queries contain unclean/special character data/words
        relevant_stemmed_query_tokens = set([token for token in relevant_stemmed_query.split() if re.search(r'[^\d]+', token)]) -stopWordsSet
        query_unigram_set.update(relevant_stemmed_query_tokens)
        
        if len(relevant_stemmed_query_tokens)==0:
            continue
        
        if relevant_stemmed_query not in stemmed_unstemmed_query_map:
            stemmed_unstemmed_query_map[relevant_stemmed_query]=[query]
            query_queryUnigram_map.append({'StemmedSearchTerm':relevant_stemmed_query,
                                           'QueryUnigrams':relevant_stemmed_query_tokens,
                                           'NumQueryUnigrams':len(relevant_stemmed_query_tokens)})
        else:
            stemmed_unstemmed_query_map[relevant_stemmed_query].append(relevant_stemmed_query)

query_df = pd.read_csv(raw_query_filepath)
query_df['SearchTerm'] = query_df['SearchTerm'].apply(remove_char_encoding)
query_df = query_df.groupby(['SearchTerm'], as_index=False).sum()
query_df.to_csv('',index=False,encoding='utf8')  #Set path for cleaned queries output dump.

query_df['StemmedSearchTerm'] = query_df['SearchTerm'].apply(remove_first_last_sc).apply(stems)
query_df = query_df.groupby(['StemmedSearchTerm','SearchTerm'], as_index=False).sum()

end_time = datetime.datetime.now()
print('Time taken to build query unigram set= {}'.format(end_time-start_time))
        
product_file_path = ''  # Input single line docs file
start_time = datetime.datetime.now()

spark = SparkSession.builder.appName('sparkApp').getOrCreate()

product_file_rdd = spark.read.text(product_file_path).rdd
print("RDD built; time taken {}".format(datetime.datetime.now()-start_time))

product_field_value_df = product_file_rdd.map(lambda line:line[0])\
.flatMap(lambda line: json.loads(line).items())\
.flatMap(split_to_unigrams).groupByKey().map(lambda x: (x[0], set([unigram for unigram in x[1]])))\
.filter(lambda x: x[0] in relevance_config_dict)

print("Restructured product data; time taken {}".format(datetime.datetime.now()-start_time))

matched_field_unigram_map = product_field_value_df.collectAsMap()

matched_unigram_set =  set()

for field,unigram_set in matched_field_unigram_map.items():
    relevant_unigram_set = set()
    for unigram in unigram_set:
        if re.search(r'[^\d]+', unigram):
            relevant_unigram_set.add(unigram)
    matched_unigram_set.update(relevant_unigram_set)
#matched_unigram_set = set(product_field_value_df.keys().collect())-stopWordsSet
matched_unigram_set = matched_unigram_set - stopWordsSet -set([''])
num_matched_unigrams = len(matched_unigram_set)

print("Matched unigram set built; time taken {}".format(datetime.datetime.now()-start_time))

for element in query_queryUnigram_map:
    temp_overlap_set = element['QueryUnigrams'].intersection(matched_unigram_set)
    element['Overlap'] = temp_overlap_set
    element['OverlapLen'] = len(temp_overlap_set)
    temp_overlap_percent = element['OverlapLen']/element['NumQueryUnigrams']
    element['OverlapPercent'] = temp_overlap_percent
    
    if temp_overlap_percent==1:
        element['OverlapBucket']='Full'
    elif temp_overlap_percent<1 and temp_overlap_percent>0:
        element['OverlapBucket']='Partial'
    else:
        element['OverlapBucket']='None'

query_queryUnigram_df = pd.DataFrame(query_queryUnigram_map)
query_df = query_queryUnigram_df.merge(query_df,on='StemmedSearchTerm')
query_df.to_csv('',index=False,encoding='utf8')  # Destination filepath to move df to csv
print("Query match stats built; time taken {}".format(datetime.datetime.now()-start_time))

