import  math, threading
import urllib2, shutil, os
from glob import iglob

# 1) ORM https://github.com/coleifer/peewee

class threaded_downloader():
    def __init__(self):
        self.set_max_download_threads(10)
        self.set_chunk_length(524288)
        self.set_working_directory('C:/Users/Paul.Admin-i7/Desktop/working/')
		self.set_output_directory(self.working_directory)
        self.set_callback(self.callback)
        self.set_output_file_name('joined.mp4')

    def join_parts(self):
        destination = open(''.join((self.output_directory, self.output_file_name)), 'wb')
        for filename in iglob(os.path.join(self.working_directory, '*.part')):
            shutil.copyfileobj(open(filename, 'rb'), destination)
            os.remove(filename)
        destination.close()

    def set_output_file_name(self, file_name):
        self.output_file_name = file_name
        
    def callback(self):
        print("Download complete, file: {f}" .format(f=''.join((self.output_directory, self.output_file_name)))

    def set_max_download_threads(self, max_threads):
        self.pool_sema =  threading.BoundedSemaphore(value=max_threads)
        
    def set_working_directory(self, working_directory):
        self.working_directory = working_directory
    
	def set_output_directory(self, output_directory):
        self.output_directory = output_directory
        
    def set_callback(self, func):
        self.callback = func
        
    def set_chunk_length(self, chunk_length):
        self.chunk_length = chunk_length
        
    def set_file_url(self, file_url):
        self.file_url = file_url
        self.set_chunk_number()
        
    def set_chunk_number(self):
        self.file_length = int(urllib2.urlopen(self.file_url).headers['content-length'])
        self.chunk_number = int(math.floor(self.file_length/self.chunk_length))
        self.final_bytes = self.file_length%self.chunk_number
        
    def download_chunk(self, chunk):
        self.pool_sema.acquire()
        req = urllib2.Request(chunk['url'])
        req.headers['Range'] = 'bytes=%s-%s' % (chunk['start'], chunk['end'])
        response = urllib2.urlopen(req)
        out_file = open(chunk['file_name'], 'wb')
        shutil.copyfileobj(response, out_file)
        self.pool_sema.release()
    
    def generate_download_chunks(self):
        chunklist = []
        previous_chunk_end = -1
        for chunk in range(self.chunk_number):
            this_chunk = dict()
            this_chunk['start'] = previous_chunk_end+1
            this_chunk['end'] = previous_chunk_end + self.chunk_length
            this_chunk['file_name'] = self.working_directory + "{number}.part".format(number=str(chunk).zfill(8))
            this_chunk['url'] = self.file_url
            previous_chunk_end = this_chunk['end']
            if chunk == self.chunk_number-1:
                this_chunk['end'] = this_chunk['end'] + self.final_bytes
            chunklist.append(this_chunk)
        return chunklist
    
    def do_download(self):
        chunklist = self.generate_download_chunks()
        threads = []
        for c in chunklist:
            t = threading.Thread(target=self.download_chunk(c))
            threads.append(t)
            t.start()
            t.join()
        
        self.join_parts()
        self.callback()