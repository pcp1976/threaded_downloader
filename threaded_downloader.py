import  math, threading, shutil, os, logging, sys
if (sys.version_info > (3, 0)):
    # Python 3 code in this block
    import urllib.request as urllib2
else:
    # Python 2 code in this block
    import urllib2

from glob import iglob

class DownloadWorker(threading.Thread):
    def __init__(self, chunk, logger, pool_sema):
        super( DownloadWorker, self ).__init__()
        self.chunk = chunk
        self.logger = logger
        self.pool_sema = pool_sema
        
    def run(self):
        self.logger.debug("{} started!".format(self.getName())) 
        self.pool_sema.acquire()
        self.logger.debug("{} lock acquired".format(self.getName())) 
        req = urllib2.Request(self.chunk['url'])
        req.headers['Range'] = 'bytes=%s-%s' % (self.chunk['start'], self.chunk['end'])
        response = urllib2.urlopen(req)
        self.logger.debug("{} URL opened".format(self.getName())) 
        try:
            out_file = open(self.chunk['file_name'], 'wb')
            self.logger.debug("{} part file opened".format(self.getName())) 
            shutil.copyfileobj(response, out_file)
            self.logger.debug("{} response copied to part file".format(self.getName())) 
        except Exception as e:
            self.logger.error(str(e))
        finally:
            out_file.close()
            self.logger.debug("{} part file closed".format(self.getName())) 
            self.pool_sema.release()
            self.logger.debug("{} lock released".format(self.getName())) 

        self.logger.debug("{} ended!".format(self.getName())) 

class threaded_downloader():
    
    def __init__(self):
        self.set_max_download_threads(t_max)
        self.set_chunk_length(1920000)
        self.set_working_directory('C:/working/')
        self.set_output_directory(self.working_directory)
        self.set_callback(self.callback)
        self.set_output_file_name('joined.mp4')
        self.log_level = 'WARNING'
        self.log_directory = self.working_directory
        self.build_logger("threaded_downloader", self.log_level)
        self.logger = logging.getLogger("threaded_downloader")

    def set_log_directory(self, dir_path):
        self.log_directory = dir_path
        self.logger.handlers = []
        self.build_logger("threaded_downloader", self.log_level) # attach new handlers
    
    def build_logger(self, name, level):
        logger = logging.getLogger(name)
        logger.setLevel(level)
        if not logger.handlers:
            fh = logging.FileHandler( self.log_directory + name + '.log')
            fh.setLevel(level)
            ch = logging.StreamHandler()
            ch.setLevel(level)
            #formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)
            logger.addHandler(fh)
            logger.addHandler(ch)
    
    def join_parts(self):
        self.logger.debug("join_parts")
        try:
            f = ''.join((self.output_directory, self.output_file_name))
            self.logger.debug("join_parts : attempt to open {f}".format(f=f))
            destination = open(f, 'wb')
            self.logger.debug("join_parts : opened {f}".format(f=f))
            for filename in iglob(os.path.join(self.working_directory, '*.part')):
                self.logger.debug("join_parts : appending {f}".format(f=filename))
                shutil.copyfileobj(open(filename, 'rb'), destination)
                self.logger.debug("join_parts : removing {f}".format(f=filename))
                os.remove(filename)
                self.logger.debug("join_parts : done {f}".format(f=filename))

        except Exception as e:
            self.logger.error(str(e))
        finally:
            try:
                destination.close()
            except Exception as e:
                self.logger.error(str(e))
                
    def set_output_file_name(self, file_name):
        self.output_file_name = file_name

    def callback(self):
        self.logger.info("callback : completed {f}".format(f=self.output_file_name))

    def set_max_download_threads(self, max_threads):
        self.max_threads = max_threads
        self.pool_sema = threading.BoundedSemaphore(self.max_threads)

    def set_working_directory(self, working_directory):
        self.working_directory = working_directory
    
    def set_output_directory(self, output_directory):
        self.output_directory = output_directory

    def set_callback(self, func):
        self.callback = func

    def set_chunk_length(self, chunk_length):
        self.chunk_length = chunk_length

    def set_file_url(self, file_url):
        if(self.rattle_the_door(file_url)):
            self.file_url = file_url
            self.set_chunk_number()

    def rattle_the_door(self, url):
        try:
            urllib2.urlopen(url)
            self.logger.info("Successfully tested {url}".format(url=url))
            return True
        except Exception as e:
            self.logger.error(str(e))
            return False
        
    def set_chunk_number(self):
        self.file_length = int(urllib2.urlopen(self.file_url).headers['content-length'])
        self.logger.debug("set_chunk_number file_length={file_length}".format(file_length=self.file_length))
        self.chunk_number = int(math.floor(self.file_length/self.chunk_length))
        self.logger.debug("set_chunk_number chunk_number={chunk_number}".format(chunk_number=self.chunk_number))
        self.final_bytes = self.file_length%self.chunk_length      
        self.logger.debug("set_chunk_number final_bytes={final_bytes}".format(final_bytes=self.final_bytes))
        
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
        x = -1
        for c in chunklist:
            x = x + 1
            t = DownloadWorker(c, self.logger, self.pool_sema)
            self.logger.debug("spawned a new thread with chunk {}".format(str(c)))
            t.setName("Thread-{}".format(x))
            self.logger.debug("spawned a new thread with name Thread-{}".format(str(x)))
            threads.append(t)
            self.logger.debug("added Thread-{} to threads".format(str(x)))
        for t in threads:
            self.logger.debug("starting {}".format(str(t)))
            t.start()
            self.logger.debug("started {}".format(str(t)))
          
        for t in threads:
            self.logger.debug("calling join on {}".format(str(t)))           
            t.join()
        
        self.join_parts()
        self.callback()