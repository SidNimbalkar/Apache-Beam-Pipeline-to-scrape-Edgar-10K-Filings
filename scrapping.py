import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from urllib.request import urlopen
import time
import csv
import sys
from collections import defaultdict
import pandas as pd
import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import spacy
from spacy.tokenizer import Tokenizer
from spacy.lang.en import English
from collections import Counter
from google.cloud import storage

from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.pipeline import StandardOptions

nltk.download('stopwords')
nltk.download('punkt')

options = PipelineOptions()
#options.view_as(SetupOptions).save_main_session = True
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'caseone'
# google_cloud_options.job_name = 'recenttest564'
google_cloud_options.staging_location = 'gs://buckettest456/staging'
google_cloud_options.temp_location = 'gs://buckettest456/temp'
options.view_as(StandardOptions).runner = 'DataflowRunner'

p = beam.Pipeline(options=options)
c = beam.Pipeline(options = options)

class Split(beam.DoFn):
    def process(self, element):
        Company, Year, Filing = element.split(",")
        return [{
            'Company': str(Company),
            'Year': str(Year),
            'Filing': str(Filing),
        }]


class Attach(beam.DoFn):

    def process(self, element):

        from urllib.request import urlopen

        url = 'https://www.sec.gov/Archives/edgar/full-index/%s/QTR1/master.idx' % (element['Year'])

        response = urlopen(url)

        string_match1 = 'edgar/data/'
        element2 = 'NA'
        element3 = 'NA'
        element4 = 'NA'

        for lin in response:
            line = lin.decode()
            if element['Company'] in line and element['Filing'] in line:
                for e in line.split(' '):
                    if string_match1 in e:
                        element2 = e.split('|')
                        for element3 in element2:
                            if string_match1 in element3:
                                element4 = element3

        url3 = 'https://www.sec.gov/Archives/' + element4

        element.update([('Link', url3)])
        element['Link'] = element['Link'].rstrip()

        return [{
            'Company': element['Company'],
            'Year': element['Year'],
            'Filing': element['Filing'],
            'Link': element['Link'],
        }]


class Preprocess(beam.DoFn):

    def process(self, element):

        import nltk
        from nltk.corpus import stopwords
        from nltk.tokenize import word_tokenize
        from urllib.request import urlopen
        nltk.download('stopwords')
        nltk.download('punkt')

        str_response = urlopen(element['Link']).read().decode('utf-8')
        stop_words = set(stopwords.words('english'))
        word_tokens = word_tokenize(str_response)

        filtered_sentence = [w for w in word_tokens if not w in stop_words]
        filtered_sentence = []
        for w in word_tokens:
            if w not in stop_words:
                filtered_sentence.append(w)

        fresponse = [word for word in filtered_sentence if word.isalpha()]

        element.update([('WordList', fresponse)])

        return [{
            'Company': element['Company'],
            'Year': element['Year'],
            'Filing': element['Filing'],
            'Link': element['Link'],
            'WordList': element['WordList'],
        }]


class NLTKTokenizer(beam.DoFn):
    def process(self, element):
        import nltk
        from nltk.corpus import stopwords
        from nltk.tokenize import word_tokenize
        fdist = nltk.FreqDist(element['WordList'])

        words = []
        frequencies = []

        for word, frequency in fdist.most_common(1000000):
            words.append(word)
            frequencies.append(frequency)

        element.update([('WordList', words)])
        element.update([('Frequency', frequencies)])

        return [{
            'Company': element['Company'],
            'Year': element['Year'],
            'Filing': element['Filing'],
            'Link': element['Link'],
            'WordList': element['WordList'],
            'Frequency': element['Frequency'],
        }]


"""class Spacy(beam.DoFn):
    def process(self, element):
        import spacy
        from spacy.tokenizer import Tokenize
        from spacy.lang.en import English

        nlp = English()

        nlp.max_length = 15000000
        doc = nlp(str(element['WordList']))

        words = [token.text for token in doc if token.is_stop != True and token.is_punct != True]
        word_freq = Counter(words)
        common_words = word_freq.most_common(100000)

        element.update([('WordList', common_words)])

        return [{
            'Company': element['Company'],
            'Year': element['Year'],
            'Filing': element['Filing'],
            'Link': element['Link'],
            'WordList': element['WordList'],
        }]"""


class ProcessWords(beam.DoFn):

    def process(self, element,file1,file2,file3,file4,file5,file6,file7):
        
        import csv
        import apache_beam as beam
        from apache_beam.options.pipeline_options import PipelineOptions
        from apache_beam.io import ReadFromText
        from apache_beam.io import WriteToText
        from urllib.request import urlopen
        import time
        import csv
        import sys
        from collections import defaultdict
        import pandas as pd
        import re
        import nltk
        from nltk.corpus import stopwords
        from nltk.tokenize import word_tokenize
    
        
        company = ()
        year = ()
        filing = ()
        link = ()
        wordlist = ()
        frequency = ()
        wordtype = ()

        pword = file1
        
        for i in range(0, len(element['WordList'])):
            for word in pword:
                if word.lower() == (element['WordList'][i]).lower():
                    company += (element['Company'],)
                    year += (element['Year'],)
                    filing += (element['Filing'],)
                    link += (element['Link'],)
                    wordlist += (element['WordList'][i],)
                    frequency += (element['Frequency'][i],)
                    wordtype += ('Positive',)
        
    
        nword = file2
        
        for i in range(0, len(element['WordList'])):
            for word in nword:
                if word.lower() == (element['WordList'][i]).lower():
                    company += (element['Company'],)
                    year += (element['Year'],)
                    filing += (element['Filing'],)
                    link += (element['Link'],)
                    wordlist += (element['WordList'][i],)
                    frequency += (element['Frequency'][i],)
                    wordtype += ('Negative',)
        
        
        uword = file3
        
        for i in range(0, len(element['WordList'])):
            for word in uword:
                if word.lower() == (element['WordList'][i]).lower():
                    company += (element['Company'],)
                    year += (element['Year'],)
                    filing += (element['Filing'],)
                    link += (element['Link'],)
                    wordlist += (element['WordList'][i],)
                    frequency += (element['Frequency'][i],)
                    wordtype += ('Uncertainity',)
                    
                    
        lword = file4
        
        for i in range(0, len(element['WordList'])):
            for word in lword:
                if word.lower() == (element['WordList'][i]).lower():
                    company += (element['Company'],)
                    year += (element['Year'],)
                    filing += (element['Filing'],)
                    link += (element['Link'],)
                    wordlist += (element['WordList'][i],)
                    frequency += (element['Frequency'][i],)
                    wordtype += ('Litigious',)
        
        
        sword = file5
        
        for i in range(0, len(element['WordList'])):
            for word in sword:
                if word.lower() == (element['WordList'][i]).lower():
                    company += (element['Company'],)
                    year += (element['Year'],)
                    filing += (element['Filing'],)
                    link += (element['Link'],)
                    wordlist += (element['WordList'][i],)
                    frequency += (element['Frequency'][i],)
                    wordtype += ('Strong Modal',)
        
        
        wword = file6
        
        for i in range(0, len(element['WordList'])):
            for word in wword:
                if word.lower() == (element['WordList'][i]).lower():
                    company += (element['Company'],)
                    year += (element['Year'],)
                    filing += (element['Filing'],)
                    link += (element['Link'],)
                    wordlist += (element['WordList'][i],)
                    frequency += (element['Frequency'][i],)
                    wordtype += ('Weak Modal',)
        
        cword = file7
        
        for i in range(0, len(element['WordList'])):
            for word in cword:
                if word.lower() == (element['WordList'][i]).lower():
                    company += (element['Company'],)
                    year += (element['Year'],)
                    filing += (element['Filing'],)
                    link += (element['Link'],)
                    wordlist += (element['WordList'][i],)
                    frequency += (element['Frequency'][i],)
                    wordtype += ('Constraining',)
                    
        ## Appending words,frequencies and word type to output dictionaries
        element.update([('Company', company)])
        element.update([('Year', year)])
        element.update([('Filing', filing)])
        element.update([('Link', link)])
        element.update([('Word', wordlist)])
        element.update([('Frequency', frequency)])
        element.update([('WordType', wordtype)])

        return[{
                'Company': element['Company'],
                'Year': element['Year'],
                'Filing': element['Filing'],
                'Link': element['Link'],
                'Word': element['Word'],
                'WordType': element['WordType'],
                'Frequency': element['Frequency'],
            }]

class blobber(beam.DoFn):
   
    from google.cloud import storage
    def upload_blob(blobberbuck, source_file_name, my_blobber):
        """Uploads a file to the bucket."""
        bucket_name = "blobberbuck"
        # source_file_name = "local/path/to/file"
        # destination_blob_name = "my_blobber"

        storage_client = storage.Client()
        bucket = storage_client.bucket(blobberbuck)
        blob = bucket.blob(my_blobber)

        blob.upload_from_filename(source_file_name)

        print(
            "File {} uploaded to {}.".format(
                source_file_name, destination_blob_name
            )
        )
        
class WriteToCSV(beam.DoFn):
    def process(self,element):
     
       result = []
       result.append("{},{},{},{},{},{}".format('Company','Year','Filing','Word','WordType','Frequency'))
       for i in range(0,len(element['Word'])):
            result.append("{},{},{},{},{},{}".format(element['Company'][i],element['Year'][i],element['Filing'][i],element['Word'][i],element['WordType'][i],element['Frequency'][i]))
            
       return result

class WriteToCSVmeta(beam.DoFn):
    def process(self,element):

        result = []
        result.append("{},{},{},{}".format('Company','Year','Filing','Link'))
        result.append("{},{},{},{}".format(element['Company'],element['Year'],element['Filing'],element['Link']))
             
        return result




        
    
datap = (p|'ReadPositive' >> beam.io.ReadFromText('gs://myinputbucket/positivelist.csv'))
        
datan = (p|'ReadNegative' >> beam.io.ReadFromText('gs://myinputbucket/negativelist.csv'))

datau = (p|'ReadUncertain' >> beam.io.ReadFromText('gs://myinputbucket/uncertainlist.csv'))

datal = (p|'ReadLitigious' >> beam.io.ReadFromText('gs://myinputbucket/litigiouslist.csv'))

datas = (p|'Readstrongmodal' >> beam.io.ReadFromText('gs://myinputbucket/strongmodallist.csv'))

dataw = (p|'Readweakmodal' >> beam.io.ReadFromText('gs://myinputbucket/weakmodallist.csv'))

datac = (p|'ReadConstraining' >> beam.io.ReadFromText('gs://myinputbucket/constraininglist.csv'))

    


data_from_source = (p
                    | 'ReadCsv' >> beam.io.ReadFromText('gs://myinputbucket/testlist.csv', skip_header_lines=1)
                    | 'Splitting' >> beam.ParDo(Split())
                    | 'Attach' >> beam.ParDo(Attach()))
                    
processing=(data_from_source| 'Preprocess' >> beam.ParDo(Preprocess())
                    | 'NLTKTokenizer' >> beam.ParDo(NLTKTokenizer())
                    | 'Process' >> beam.ParDo(ProcessWords(),beam.pvalue.AsList(datap),beam.pvalue.AsList(datan),beam.pvalue.AsList(datau),
beam.pvalue.AsList(datal),beam.pvalue.AsList(datas),beam.pvalue.AsList(dataw),beam.pvalue.AsList(datac)))


csvc = (processing| 'CSVConversion' >> beam.ParDo(WriteToCSV()))


final = (csvc| 'Writing to File' >>beam.io.WriteToText('gs://buckettest456/CIK/YEAR/FILING/output.csv'))
                    
                    
                    
metadata = (data_from_source |'CSVConversionMeta' >> beam.ParDo(WriteToCSVmeta())|'Writing To File' >> beam.io.WriteToText('gs://buckettest456/CIK/YEAR/FILING/metadata.csv'))

                    
result = p.run().wait_until_finish()

