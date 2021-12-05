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

delete_fields = ['time_to_warehouse','show_in_mobile_categories','copy_cat_l3_id','copy_cat_l1_id',
                 'attr_select_min_sale_qty','copy_cat_l1','copy_cat_l2','copy_cat_l3','is_in_stock',
                 'copy_cat_l4','copy_cat_meta','copy_cat_meta_id','store_id','copy_cat_l2_id','copy_cat_l4_id',
                 'show_in_app_categories','copy_attr_select_beanbag_type','copy_attr_select_beanbag_size',
                 'copy_price']

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
    """Yields a tuple of key-value pair (field-name, field-value) along with uniqueId"""
    product_dict = json.loads(product_dict)
    product_id  = product_dict['uniqueId']
    product_dict_items = product_dict.items()
    for item in product_dict_items:
        yield((item,product_id))
        
def build_unigram_field_product_map(row):
    """Return tuple of unigram and dictionary of field product map"""
    unigram = row[0]
    field_product_map_list = row[1]
    consolidated_dict = {}
    for field_product_map in field_product_map_list:
        field = str(field_product_map[0])
        products = field_product_map[1]
        consolidated_dict[field]=products
    return((unigram,consolidated_dict))

def split_to_unigrams_v2(row):
    """Yields a tuple of unigram-field along with uniqueId"""
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
    """Assigns field properties by unigram. The same is aggregated to arrive at the searchable fields.
        Returns a dictionary with field properties mentioned below for every unigram-field combination."""
    unigram=stems(remove_first_last_sc(row[0]))
    consolidated_dict = row[1]
    for i,(field,product_set) in enumerate(sorted(consolidated_dict.items(), key=lambda x:len(x[1]), reverse=True)):
        field_rank=i+1
        if(i==0):
            top_field=field
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
    """Field properties made at a unigram level and grouped and aggregated. Marks the end of the flow."""
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

start_time = datetime.datetime.now()

# Search term dump filepath- assumed to be a csv table with schema: search_term, hits, clicks, and other metrics.
raw_query_filepath = "../Pepperfry_GASearchTermData.csv"

# Build necessary mappings and unique query unigrams set
stemmed_unstemmed_query_map = {}
query_queryUnigram_map = []
query_unigram_set = set()

with open(raw_query_filepath, 'r', encoding='utf8', errors='ignore') as raw_query_file:
    reader = csv.reader(raw_query_file)
    for i,row in enumerate(reader):
        if(i==0):  # Skip header row
            continue
        query = row[0] 
        # Arrive at clean (handle character encodings/special characters at start of string or end) stemmed query
        relevant_stemmed_query = stems(remove_first_last_sc(remove_char_encoding(query))) #re.sub(r'[\s]+', ' ', ' '.join([stems(remove_first_last_sc(token)) for token in remove_first_last_sc(remove_char_encoding(query)).split()]).strip()) ##-- a stricter method to form stemmed queries- use if expected raw queries contain unclean/special character data/words
        
        # Pick the stemmed query tokens
        relevant_stemmed_query_tokens = set([token for token in relevant_stemmed_query.split() if re.search(r'[^\d]+', token)]) -stopWordsSet
        
        # Update query unigram set
        query_unigram_set.update(relevant_stemmed_query_tokens)
        
        if len(relevant_stemmed_query_tokens)==0:
            continue
        
        # Build mapping
        if relevant_stemmed_query not in stemmed_unstemmed_query_map:
            stemmed_unstemmed_query_map[relevant_stemmed_query]=[query]
            query_queryUnigram_map.append({'StemmedSearchTerm':relevant_stemmed_query,
                                           'QueryUnigrams':relevant_stemmed_query_tokens,
                                           'NumQueryUnigrams':len(relevant_stemmed_query_tokens)})
        else:
            stemmed_unstemmed_query_map[relevant_stemmed_query].append(query)

# Read query dump data in df, clean and store the same.
query_df = pd.read_csv(raw_query_filepath)
query_df['SearchTerm'] = query_df['SearchTerm'].apply(remove_char_encoding)
query_df = query_df.groupby(['SearchTerm'], as_index=False).sum()
query_df.to_csv('Pepperfry_CleanedQueries.csv',index=False,encoding='utf8')

query_df['StemmedSearchTerm'] = query_df['SearchTerm'].apply(remove_first_last_sc).apply(stems)
query_df = query_df.groupby(['StemmedSearchTerm','SearchTerm'], as_index=False).sum()

end_time = datetime.datetime.now()
print('Time taken to build query unigram set= {}'.format(end_time-start_time))

# Enter the single line docs filepath
product_file_path = '../PepperfryFeed_SingleLineDocs'
start_time = datetime.datetime.now()

# Start spark session
spark = SparkSession.builder.appName('sparkApp').getOrCreate()

# Build single line docs' RDD or product file RDD
product_file_rdd = spark.read.text(product_file_path).rdd
print("RDD built; time taken {}".format(datetime.datetime.now()-start_time))

# Restructure the RDD
product_field_value_df = product_file_rdd.map(lambda line:line[0]).flatMap(product_dict_preprocessing)
print("Restructured product data; time taken {}".format(datetime.datetime.now()-start_time))

# Restructure RDD to arrive at product inverted index- a unigram, field, product mapping.
product_inverted_index = product_field_value_df.flatMap(split_to_unigrams_v2)\
.reduceByKey(lambda a,b:a+b).map(lambda x: (stems(remove_first_last_sc(x[0][0])),(x[0][1], set(x[1])))).groupByKey()\
.map(build_unigram_field_product_map)

# Filter for queries in query unigram set and delete fields as in delete fields list- remove fields which contain conflicting/irrelevant data from pipeline, e.g. description
filtered_product_inverted_index=product_inverted_index.filter(lambda x: stems(remove_first_last_sc(x[0])) in query_unigram_set).map(filter_fields)
print("Filtered product inverted index built; time taken {}".format(datetime.datetime.now()-start_time))

# Fetch matched unigram set
matched_unigram_set = set(filtered_product_inverted_index.keys().collect())
num_matched_unigrams = len(matched_unigram_set)
print("Matched unigram set built; time taken {}".format(datetime.datetime.now()-start_time))

# Build query match stats
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
query_df.to_csv('Pepperfry_QueryMatchStats.csv',index=False,encoding='utf8')
print("Query match stats built; time taken {}".format(datetime.datetime.now()-start_time))

# Identify searchable fields based on the number of unigrams covered by a field
unigram_fieldProduct_map = filtered_product_inverted_index.flatMap(find_fields_byUnigram).groupByKey()\
.map(build_field_aggregations)
unigram_fieldProduct_df = unigram_fieldProduct_map.toDF(['Field','FieldRank','TopFieldFlag','RelevantFlag','NumProducts','NumUniqueProducts',
                                                         'NumProductsIntersect','UnigramsCovered','UnigramsMandatory']).toPandas()

unigram_fieldProduct_df['TotalNumUnigrams']=num_matched_unigrams
print("Unigram field product dataframe built; time taken {}".format(datetime.datetime.now()-start_time))
unigram_fieldProduct_df.to_csv("PepperfryFeed_SearchableFields.csv",encoding='utf8',index=False)