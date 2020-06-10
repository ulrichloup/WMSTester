#!/bin/python3
# Automatically generates web map service (WMS) requests.
# By Ulrich Loup (2020-05-14)
import sys
import argparse
from requests import Response, Session, Request, PreparedRequest
from random import randint
from abc import ABC, abstractmethod
from os import path
from copy import deepcopy
from time import sleep, time
from threading import active_count, Thread, Semaphore
try:
   from queue import SimpleQueue
except ImportError:
   from Queue import SimpleQueue

#
# Class definitions
#

class IOTools:
    """Collection of input/ouput methods."""
    progresstotal = 0
    progresscount = 0
    outputfile = None
    CSVseparator = ';'

    def setCSVSeparator(self, CSVseparator):
        """Set another separator for the CSV output than ";"."""
        self.CSVseparator = CSVseparator

    def setOutputFile(self, outputfile):
        """Opens an output file at the given path if the directory exists. The file is then used in the output method as target and the console output is supressed."""
        if outputfile:
            outputdir = path.dirname(outputfile)
            if outputdir.strip() and not path.exists(path.dirname(outputfile)):
                raise Exception("The output directory " + path.dirname(outputfile) + " does not exist.")
            else:
                self.outputfile = open(outputfile, "w+")

    def outputLine(self, text):
        """Output the given text in one line."""
        if self.outputfile:
            self.outputfile.write(text + '\n')
            self.outputfile.flush()
        else:
            print(text)
    
    def outputTest(self, test, outputformat):
        """Output the given test with the given outputformat being one of OUTPUT_FORMATS."""
        if outputformat == "csv":
            self.outputCSVLine([test.id, test.layers, test.result.getCSV()])
        else:
            self.outputLine(test.id + "(" + test.layers + " + " + test.boundingbox.__str__() + ")" + (": " + str(test.result.response.elapsed.total_seconds()) + " sec" if test.result else ""))
        test.result.close()
    
    def outputCSVLine(self, blocks):
        """Output the given list of text blocks in one CSV line."""
        if not isinstance(blocks, list):
            raise Exception("The text blocks must be provided as list: " + blocks)
        csvoutput = ""
        for b in blocks:
            csvoutput += b.__str__() + self.CSVseparator
        self.outputLine(csvoutput[:-(self.CSVseparator.__len__())])
        
    def initProgress(self, total):
        """Initializes the total number of steps for the progress bar."""
        self.progresstotal = total
        self.progresscount = 0

    def progress(self, increment = 1, status=''):
        """Progress bar from https://gist.github.com/vladignatyev/06860ec2040cb497f0f3. Increments the count by increment (default=1)."""
        bar_len = 60
        filled_len = int(round(bar_len * self.progresscount / float(self.progresstotal)))
        percents = round(100.0 * self.progresscount / float(self.progresstotal), 1)
        bar = '=' * filled_len + '-' * (bar_len - filled_len)
        sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
        sys.stdout.flush()
        self.progresscount += increment
    
    def close(self):
        """Closes all files and streams of this IOTools object."""
        if(self.outputfile): self.outputfile.close()


class Box:
    """Represents a two-dimensional box."""
    lowerx = 0
    lowery = 0
    upperx = 1
    uppery = 1
    
    def __init__(self, lowerx=0, lowery=0, upperx=1, uppery=1):
        """Initiates the box by giving the lower left and the upper right points (default: unit box at (0,0))."""
        self.lowerx = lowerx
        self.lowery = lowery
        self.upperx = upperx
        self.uppery = uppery
    
    def __str__(self):
        """Returns a string representation of the box."""
        return self.lowerx.__str__() + "," + self.lowery.__str__() + "," + self.upperx.__str__() + "," + self.uppery.__str__()
        
    def contains(self, box):
        """Returns True if and only if the given box is within the borders of this box."""
        return box.lowerx >= self.lowerx and box.lowery >= self.lowery and box.upperx <= self.upperx and box.uppery <= self.uppery

    def generateRandomSubbox(self, minwidth=1.0, minheight= 1.0, maxfractionaldigits=0):
        """Returns a new random box inside this box. Optinally, minwidth (default: 1.0), minheight (default: 1.0), maxfractionaldigits (default: 0) can be given."""
        digitsx = randint(0, maxfractionaldigits)
        if digitsx:
            randx = randint(self.lowerx, (self.upperx-minwidth) * pow(10, digitsx))
            randwidth = randint(minwidth * pow(10, digitsx), self.upperx* pow(10, digitsx) - randx)
            randx /=  pow(10, digitsx)
            if randx > self.upperx-minwidth:
                randx -= 1
                randx -= minwidth
            randwidth /=  pow(10, digitsx)
        else:
            randx = randint(self.lowerx, (self.upperx-minwidth))
            randwidth = randint(minwidth, self.upperx - randx)

        digitsy = randint(0, maxfractionaldigits)
        if digitsy:
            randy = randint(self.lowery, (self.uppery-minheight) * pow(10, digitsy))
            randheight = randint(minheight * pow(10, digitsy), self.uppery * pow(10, digitsy) - randy)
            randy /= pow(10, digitsy)
            if randy > self.uppery-minheight:
                randy -= 1
                randy -= minheight
            randheight /=  pow(10, digitsy)
        else:
            randy = randint(self.lowery, (self.uppery-minheight))
            randheight = randint(minheight, self.uppery - randy)
        return Box(randx, randy, randx+randwidth, randy+randheight)

    def shiftX(self, step):
        """Shifts the box by step on the x axis."""
        self.lowerx = self.lowerx + step
        self.upperx = self.upperx + step

    def shiftY(self, step):
        """Shifts the box by step on the y axis."""
        self.lowery = self.lowery + step
        self.uppery = self.uppery + step
    
    def zoom(self, step):
        """Zooms this box by the given step starting from the center point, i.e., the width and the height of the box are expanded or shrinked by step into each direction."""
        self.lowerx -= step
        self.lowery -= step
        self.upperx += step
        self.uppery += step
        

class WMSServer:
    """Represents a web map service (WMS) server and provides methods to connect to it."""
    host = "localhost"
    port = 7600
    path = "/wms"
    
    def __init__(self, host="localhost", port=7600, path="/wms"):
        """Initializes a WMS at the given host, port and path (default: localhost:7600/wms). Host and path are normalized."""
        pIndex = host.find("://")
        if pIndex != -1:
            host = host[pIndex+3:]
        self.host = host
        self.port = port
        if path.startswith("/"):    
            self.path = path
        else:
            self.path = "/" + path
    
    def generateURL(self, protocol = "http"):
        """Generates the URL to the WMS server. The default protocol "http" can be overridden by supplying the parameter protocol."""
        return protocol + "://" + self.host + ":" + self.port.__str__() + self.path
    
    def __str__(self):
        """Returns the default URL."""
        return self.generateURL()


class WMSTestResult:
    """Stores the result data of a WMSTest."""
    request = None
    """The prepared request object."""
    response = None
    """The response object."""
    
    def __init__(self, request, response=None):
        """Initializes a WMSTestResult with a request object and optinally with a response object."""
        if not isinstance(request, PreparedRequest):
            raise Exception("The parameter request must be a requests.Request object.")
        self.request = request
        if response and not isinstance(response, Response):
            raise Exception("The parameter response must be a requests.Response object.")
        self.response = response
    
    def getCSV(self):
        """Generates a CSV representation of the test result."""
        # if r.headers["Content-Type"] != "image/png":
        # raise Exception("Unexpected response format", r.text)
        if self.response:
            return self.request.url + ";" + str(self.response.status_code) + ";" + self.response.headers["Content-Type"] + ";" + str(self.response.elapsed.total_seconds())
        return self.request.url
        
    def close(self):
        """Closes the response."""
        self.response.close()
        

class WMSTest(ABC):
    """A WMS test fixes the basic parameters for a call to a WMS: WMSServer, request, version. Aditional parameters can be added."""
    server = None
    layers = ""
    width = 0
    height = 0
    boundingbox = Box(-180.0, -90.0, 180.0, 90.0)
    basicparameters = {}
    result = None
    id = "WMSTest"
    """a short string uniquely identifying the test"""
    spatialextent = Box(-180.0, -90.0, 180.0, 90.0)
    
    def __init__(self, server, layers, width, height):
        """Initializes a WMS test with the given id and WMSServer server. Aditionally, the srs="EPSG:4326", format="image/png", bbox=-180,-90,180,90 is set."""
        if not isinstance(server, WMSServer):
            raise Exception("The server must be an instance of WMSServer.")
        self.server = server
        self.result = None
        self.basicparameters["service"] = "WMS"
        self.basicparameters["version"] = "1.1.0"
        self.basicparameters["request"] = "GetMap"
        self.setSRS("EPSG:4326")
        self.setFormat("image/png")
        self.layers = layers
        self.width = width
        self.height = height

    def clone(self):
        """Returns a copy of this class object while setting the given parameters differently."""
        return deepcopy(self)

    def setBasicParameter(self, key, value):
        """Set a basic parameter of the WMS test, which is shared between all copys of this WMS test. The parameter is added if it does not exist. The parameter will be used in every request."""
        self.basicparameters[key] = value
        return self
    
    def setLayers(self, layers):
        """Define the parameter layers. This parameter will be added to any request of this test."""
        self.layers = layers
        return self
        
    def setWidth(self, width):
        """Define the parameter width. This parameter will be added to any request of this test."""
        self.width = width
        return self
        
    def setHeight(self, height):
        """Define the parameter height. This parameter will be added to any request of this test."""
        self.height = height
        return self
        
    def setBoundingBox(self, box):
        """Re-defines the bounding box parameter by the given box."""
        if not isinstance(box, Box):
            raise Exception("The given box must be of type Box: " + box)
        if not self.spatialextent.contains(box):
            raise Exception("The given box must be inside the spatial extent " + self.spatialextent + ".")
        self.boundingbox = box
        return self

    def setSpatialExtent(self, box):
        """Sets the bounding box in which the testing boxes are generated by the given box."""
        if not isinstance(box, Box):
            raise Exception("The given box must be of type Box: " + box)
        self.spatialextent = box
        return self

    def setSRS(self, srs):
        """Sets the srs (spatial reference system) parameter. Default is "EPSG:4326"."""
        self.basicparameters["srs"] = srs
        return self

    def setFormat(self, requestformat):
        """Sets the format parameter. Default is "image/png"."""
        self.basicparameters["format"] = requestformat
        return self

    def createRequest(self, params = {}):
        """Sends a request to the WMS server using the basic and optinally the given parameters in params."""
        params.update(self.basicparameters)
        params["layers"] = self.layers
        params["width"] = self.width
        params["height"] = self.height
        params["bbox"] = self.boundingbox.__str__()
        return Request('GET', self.server, params=params).prepare()
        
    def execute(self, dry = False, verbosity=0, session=None):
        """This method executes the test and stores its WMSTestResult object which is available by the property result. If the optional second parameter dry is True, the test does not send requests. Additionally, the third parameter verbosity can be set to a non-negative integer representing the verbosity level (default: 0). The fourth parameter s can be used to provide a Session for re-use. Otherwise each request opens and closes its own session."""
        r = self.createRequest()
        if dry:
            self.result = WMSTestResult(r)
        else:
            try:
                if not session or not isinstance(session, Session):
                    self.result = WMSTestResult(r, session.send(r, verify=False))
                else:
                    session = Session()
                    self.result = WMSTestResult(r, session.send(r, verify=False))
                    session.close()
            except Exception as e:
                if e.__str__().__contains__("busy"):
                    if verbosity: print("Pausing the request sending for 5 sec due to a connection overflow.")
                    sleep(5)
                    self.result = WMSTestResult(r, session.send(r, verify=False))
                else:
                    raise Exception("Error while sending http request: {0}".format(e))

    def getCSV(self):
        """Generates a CSV representation of the test result."""
        return self.basicparameters["bbox"]

    def __str__(self):
        """Generates a string representation of the WMSTest."""
        return self.server.__str__() + " " + self.basicparameters.__str__()


class RandomBoundingBoxWMSTest(WMSTest):
    """Generates a box inside the given bounding box with a random lower left corner and random width and heigth."""
    id = "RandomBbox"
    minwidth = 1.0
    minheight = 1.0
    maxfractionaldigits = 3
    # Internally, a point is represented as a pair of doubles and a box as pair of the lower left and the upper right points.

    def __init__(self, server, layers, width, height):
        """Initiates a RandomBoundingBoxWMSTest by generating a random bounding box."""
        super(RandomBoundingBoxWMSTest, self).__init__(server, layers, width, height)
        self.generateRandomBoundingBox()
    
    def setMaxFractionalDigits(self, maxfractionaldigits):
        """Sets the maximum number of fractional digits in any random number generated."""
        self.maxfractionaldigits = maxfractionaldigits
        return self
    
    def setMinwidth(self, minwidth):
        """Sets the minimum width of the random box."""
        self.minwidth = minwidth
        return self
    
    def setMinheight(self, minheight):
        """Sets the minum height of the random box."""
        self.minheight = minheight
        return self

    def generateRandomBoundingBox(self):
        """(Re-)generates a random box inside the spatial extent."""
        self.setBoundingBox(super().spatialextent.generateRandomSubbox(self.minwidth, self.minheight, self.maxfractionaldigits))
        
        
class WalkingBoundingBoxWMSTest(RandomBoundingBoxWMSTest):
    """Moves a given box by a random step width on the x and on the y axis."""
    id = "WalkingBbox"
    minstepwidth = 1.0
    maxstepwidth = 12.0
    
    def __init__(self, server, layers, width, height):
        """Initiates a WalkingBoundingBoxWMSTest by generating a random bounding box."""
        super(WalkingBoundingBoxWMSTest, self).__init__(server, layers, width, height)
    
    def moveBoundingBox(self, xstep = 0, ystep = 0):
        """Moves the bounding box by one step on the x and on the y axis. If the parameters xstep and ystep are not given, the steps are randomly chosen between minstepwidth and maxstepwidth."""
        direction = randint(0, 1)
        if xstep == 0:
            maxx = int((self.spatialextent.upperx - self.boundingbox.upperx) if direction else self.boundingbox.lowerx - self.spatialextent.lowerx)
            xstep = randint(min(self.minstepwidth, maxx), min(self.maxstepwidth, maxx))
            xstep = xstep if direction else -xstep
            self.boundingbox.shiftX(xstep)
        else:
            self.boundingbox.shiftX(xstep)
            if not self.spatialextent.contains(self.boundingbox):
                self.boundingbox.shiftX(-xstep)
                raise Exception("The given x step moves the box out of the spatial extent: " + xstep)
        direction = randint(0, 1)
        if ystep == 0:
            maxy = int((self.spatialextent.uppery - self.boundingbox.uppery) if direction else self.boundingbox.lowery - self.spatialextent.lowery)
            ystep = randint(min(self.minstepwidth, maxy), min(self.maxstepwidth, maxy))
            ystep = ystep if direction else -ystep
            self.boundingbox.shiftY(ystep)
        else:
            self.boundingbox.shiftY(ystep)
            if not self.spatialextent.contains(self.boundingbox):
                self.boundingbox.shiftY(-ystep)
                raise Exception("The given y step moves the box out of the spatial extent: " + ystep)
        return self


class ZoomingBoxWMSTest(RandomBoundingBoxWMSTest):
    """Moves a given box by a random step width on the x and on the y axis."""
    id = "ZoomingBbox"
    minboxwidth = 5.0
    maxboxwidth = 180.0
    
    def __init__(self, server, layers, width, height):
        """Initiates a ZoomingBoxWMSTest by generating a random bounding box."""
        super(ZoomingBoxWMSTest, self).__init__(server, layers, width, height)
    
    def zoomBoundingBox(self, step = 0):
        """Zooms the bounding box by one step starting from the center. If the parameter step is not given, the step is randomly chosen such that the zoomed box has a width between minboxwidth and the maximum box width."""
        if step == 0:
            maxstep = int(min( self.maxboxwidth, (self.spatialextent.upperx - self.boundingbox.upperx), (self.spatialextent.uppery - self.boundingbox.uppery), (self.boundingbox.lowerx - self.spatialextent.lowerx), (self.boundingbox.lowery - self.spatialextent.lowery) ))
            step = randint(min(self.minboxwidth, maxstep), max(self.minboxwidth, maxstep))
            self.boundingbox.zoom(step)
        else:
            self.boundingbox.zoom(step)
            if not self.spatialextent.contains(self.boundingbox):
                self.boundingbox.zoom(-step)
                raise Exception("The given step zooms the box out of the spatial extent: " + step)
        return self


class WMSTestThread(Thread):
    """Encapsulates a WMSTest in a separate Thread."""
    
    test = None
    threadpool = None
    testscompleted = None
    dry = False
    verbosity = 0
    session = None
    keepalive = True
    
    def __init__(self, test, threadpool, testscompleted, dry = False, verbosity=0, session=None, keepalive=True):
        """Initializes the Thread with a WMSTest test and a Semaphore threadpool. The given test can be executed with the specified additional parameters dry (default = False), verbosity (default = 0) and session. Moreover, the parameter keepalive can be set to False (default = True) in order to close a given session after the test."""
        Thread.__init__(self)
        if not isinstance(test, WMSTest):
            raise Exception("The given test must be an instance of WMSTest.")
        self.test = test
        if not isinstance(threadpool, Semaphore):
            raise Exception("The given threadpool must be an instance of Semaphore.")
        self.threadpool = threadpool
        if not isinstance(testscompleted, SimpleQueue):
            raise Exception("The given testscompleted must be an instance of SimpleQueue.")
        self.testscompleted = testscompleted
        self.dry = dry
        self.verbosity = verbosity
        self.session = session
        self.keepalive = keepalive
    
    def run(self):
        """Executes the test."""
        try:
            self.test.execute(self.dry, self.verbosity, self.session)
        except Exception as e:
            print(e.__str__())
        if not self.keepalive:
            self.session.close()
        self.testscompleted.put(self.test)
        self.threadpool.release()

#
# Global definitions
#

OUTPUT_FORMATS = ["csv", "bboxes"]
MAX_CONNECTIONS = 256
TEST_CLASSES = [RandomBoundingBoxWMSTest.id, WalkingBoundingBoxWMSTest.id, ZoomingBoxWMSTest.id]  #["File"]


#
# Main script
#
def main():
    """Main function providing a command-line tool with a rich set of arguments."""
    # Parse arguments
    parser = argparse.ArgumentParser(description='Automatically generates web-map-service (WMS) requests and collects their response data. In particular, response times are measured (in seconds).', formatter_class=argparse.ArgumentDefaultsHelpFormatter, epilog="Example call:\npython .\wms_tester.py --host oflkpr100.webcc.dwd.de --path /geoserver/dwd/wms --width 768 --height 384 dwd:GeoRaster_Benchmark_GeoTIFF dwd:GeoRaster_Benchmark_NN_1 dwd:GeoRaster_Benchmark_NN_2 dwd:GeoRaster_Benchmark_NN_3 dwd:GeoRaster_Benchmark_NN_4 dwd:GeoRaster_Benchmark_NN_5")
    parser.add_argument("--verbose", "-v", action='count', help="produce more debugging output")
    parser.add_argument("--host", default="localhost", help="host name of the WMS server")
    parser.add_argument("--port", type=int, default=7600, help="port of the WMS server")
    parser.add_argument("--path", default="/wms", help="service path of the WMS")
    parser.add_argument("--output-format", dest="outputformat", choices=OUTPUT_FORMATS, help="format of the test result")
    parser.add_argument("--output-file", dest="outputfile", help="path to a file where the output is stored (if given, the console output is supressed")
    # parser.add_argument("--input-file", dest="inputfile", help="path to a file containing one box defined by [[lower_x,lower_y],[upper_x,upper_y]] per line")
    parser.add_argument("--width", type=int, required=True, help="width of the requested maps")
    parser.add_argument("--height", type=int, required=True, help="height of the requested maps")
    parser.add_argument("--dry-run", dest="dry", action='store_true', help="do not send, but only output the requests")
    parser.add_argument("layers", nargs='+', help="list of layer names to be tested against each other")
    parser.add_argument("--tests", nargs='+', default=TEST_CLASSES[0], choices=TEST_CLASSES)
    parser.add_argument("--count", type=int, default=1, help="positive number of test repetitions")
    parser.add_argument("--threads", type=int, default=1, help="positive number of simultaneous tests")

    iot = IOTools()
    
    args = parser.parse_args()
    # print(args)
    iot.setOutputFile(args.outputfile)
    width = args.width
    height = args.height
    verbosity = args.verbose
    layers = args.layers
    testclasses = args.tests
    count = args.count if args.count > 0 else 1
    threadpool = Semaphore(args.threads) if args.threads > 0 else Semaphore()

    if verbosity: print("Initizing tests... ", end = '')
    wmsserver = WMSServer(args.host, args.port, args.path)
    tests = [[[] for l in layers] for t in testclasses]
    for i in range(testclasses.__len__()):
        t = testclasses[i]
        print(t + " ", end = '')
        if t == RandomBoundingBoxWMSTest.id:
            # first layer determines the box
            tests[i][0] = [RandomBoundingBoxWMSTest(wmsserver, layers[0], width, height) for c in range(count)]
            for j in range(1, layers.__len__()):
                tests[i][j] = [tests[i][0][c].clone().setLayers(layers[j]) for c in range(count)]
        if t == WalkingBoundingBoxWMSTest.id:
            # first layer determines the box
            # first box starts the walk
            tests[i][0] = [WalkingBoundingBoxWMSTest(wmsserver, layers[0], width, height)]
            for c in range(0, count-1):
                tests[i][0] += [tests[i][0][c].clone().moveBoundingBox()]
            for j in range(1, layers.__len__()):
                tests[i][j] = [tests[i][0][c].clone().setLayers(layers[j]) for c in range(count)]
        if t == ZoomingBoxWMSTest.id:
            # first layer determines the box
            # first box starts the zooming
            tests[i][0] = [ZoomingBoxWMSTest(wmsserver, layers[0], width, height)]
            for c in range(0, count-1):
                tests[i][0] += [tests[i][0][c].clone().zoomBoundingBox()]
            for j in range(1, layers.__len__()):
                tests[i][j] = [tests[i][0][c].clone().setLayers(layers[j]) for c in range(count)]
    if verbosity: print("done.")
    if(verbosity == 2): print(tests)

    if verbosity:
        print("Testing... ")
        if not args.dry:
            iot.initProgress(count*testclasses.__len__()*layers.__len__())
            iot.progress()
    connectioncount = 0
    session = Session()
    testscompleted = SimpleQueue()
    # startingtime = int(time())
    for i in range(tests.__len__()):
        for j in range(tests[i].__len__()):
            if args.outputformat == "bboxes":
                iot.outputLine(tests[i][j][0].boundingbox.__str__())
            else:
                for t in tests[i][j]:
                    # start test in a thread
                    if threadpool.acquire():
                        connectioncount += 1
                        WMSTestThread(t, threadpool, testscompleted, args.dry, verbosity, session, connectioncount == MAX_CONNECTIONS).start()
                        if connectioncount == MAX_CONNECTIONS:
                            if verbosity:
                                print()
                                print("Reached " + str(connectioncount) + " connections. Renewing TCP connection.")
                            session = Session()
                            connectioncount = 0
                    # evaluate intermediate results
                    while not testscompleted.empty():
                        iot.outputTest(testscompleted.get(), args.outputformat)
                        if verbosity and not args.dry: iot.progress()
    # evaluate remaining results
    while active_count() > 1 or not testscompleted.empty():
        iot.outputTest(testscompleted.get(), args.outputformat)
        if verbosity and not args.dry: iot.progress()
        sleep(1)
    if verbosity and not args.dry: print()
    iot.close()
    if verbosity: print("done.")
    
if __name__ == '__main__':
    main()