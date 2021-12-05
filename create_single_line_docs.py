#Sample code flow to create single line docs-- required pre-processing step for the searchable fields job.
#Output of this code is to convert the feed file (processed catalog in this example) to a file wherein each line represents one product as a dictionary.
#This is the input feed file for the pyspark code.
#Further, this code also captures variant level information and copies it to be part of the parent product.


import os
import ijson
import re
import simplejson as json

input_file_path = ""  # Input feed filepath-- assumed to be JSON
input_file = open(input_file_path,'rb')

#Output destination file-- single line docs
output_file = open("","w+")

#Iterator to return all products as python objects as found under the prefix
objects = ijson.items(input_file,'feed.catalog.add.items.item')

#Iterate through the products
for j,product in enumerate(objects):
    if(j%1000==0):
        print(j)

#Copy variant information and store into a multi-valued array or list as part of parent-- delete variant information post copying    
    try:
        variants = product['variants']
        for variant in variants:
            for variant_field,variant_field_val in variant.items():
                if('variants__'+variant_field in product):
                    if(isinstance(variant_field_val, list)):
                        product['variants__'+variant_field].extend(variant_field_val)
                    else:
                        product['variants__'+variant_field].append(variant_field_val)
                else:
                    product['variants__'+variant_field] = []
                    if(isinstance(variant_field_val, list)):
                        product['variants__'+variant_field].extend(variant_field_val)
                    else:
                        product['variants__'+variant_field].append(variant_field_val)
        del product['variants']
        
    except KeyError:
        pass
    
    output_file.write(json.dumps(product))
    output_file.write("\n")
    
    
input_file.close()
output_file.close()