#!/bin/python3
# Automatically generates web map service (WMS) requests.
# By Ulrich Loup (2020-05-14)
import sys
import argparse
import requests
from random import randint
from abc import ABC, abstractmethod
from os import path

#
# Global definitions
#

OUTPUT_FORMATS = ["csv", "bboxes"]
TEST_CLASSES = ["RandomBbox","File"]


#
# Class definitions
#

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
        """Outputs a string representation of the box."""
        return self.lowerx.__str__() + "," + self.lowery.__str__() + "," + self.upperx.__str__() + "," + self.uppery.__str__()
        
    def contains(self, box):
        """Returns True if and only if the given box is within the borders of this box."""
        return box.lowerx >= self.lowerx and box.lowery >= self.lowery and box.upperx <= self.upperx and box.uppery <= self.uppery

    def shuffle(self, minwidth=1.0, minheight= 1.0, maxfractionaldigits=0):
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
    responses = []
    bboxes = []
    
    def __init__(self, responses, bboxes):
        """Initializes a WMSTestResult with a list of response objects and bounding boxes."""
        if not isinstance(responses, list) and [isinstance(r, requests.Response) for r in responses] == [True for r in responses]:
            raise Exception("The parameter responses must be a list of requests.Response objects.")
        self.responses = responses
        if not isinstance(bboxes, list) and [isinstance(b, Box) for b in bboxes] == [True for b in bboxes]:
            raise Exception("The parameter responses must be a list of Box objects.")
        self.bboxes = bboxes
    
    def getCSV(self):
        """Generates a CSV representation of the test result."""
        # if r.headers["Content-Type"] != "image/png":
        # raise Exception("Unexpected response format", r.text)
        return self.response.url + ";" + str(self.response.status_code) + ";" + self.response.headers["Content-Type"] + ";" + str(self.response.elapsed.total_seconds())


class WMSTest(ABC):
    """A WMS test fixes the basic parameters for a call to a WMS: WMSServer, request, version. Aditional parameters can be added."""
    server = ""
    result = ""
    parameters = {}
    id = ""
    """a short string uniquely identifying the test"""
    spatialextent = Box(-180.0, -90.0, 180.0, 90.0)
    
    def __init__(self, id, server, layers, width, height):
        """Initializes a WMS test with the given id and WMSServer server. Aditionally, the srs="EPSG:4326", format="image/png", bbox=-180,-90,180,90 is set."""
        self.id = id
        if not isinstance(server, WMSServer):
            raise Exception("The server must be an object of WMSServer.")
        self.server = server
        self.parameters["service"] = "WMS"
        self.parameters["version"] = "1.1.0"
        self.parameters["request"] = "GetMap"
        self.parameters["layers"] = layers
        self.parameters["width"] = width
        self.parameters["height"] = height
        self.setSRS("EPSG:4326")
        self.setFormat("image/png")
        self.setBoundingBox(Box(-180.0, -90.0, 180.0, 90.0))
    
    def addParameter(self, key, value):
        """Add another basic parameter to the EMS test. The parameter will be used in every request."""
        self.parameters[key] = value
    
    def setBoundingBox(self, box):
        """Re-defines the bounding box parameter by the given box."""
        if not isinstance(box, Box):
            raise Exception("The given box must be of type Box: " + box)
        self.parameters["bbox"] = box.__str__()

    def setSpatialExtent(self, box):
        """Sets the bounding box in which the testing boxes are generated by the given box."""
        if not isinstance(box, Box):
            raise Exception("The given box must be of type Box: " + box)
        self.spatialextent = box

    def setSRS(self, srs):
        """Sets the srs (spatial reference system) parameter. Default is "EPSG:4326"."""
        self.parameters["srs"] = srs

    def setFormat(self, requestformat):
        """Sets the format parameter. Default is "image/png"."""
        self.parameters["format"] = requestformat

    def request(self, params = {}):
        """Sends a request to the WMS server using the basic and optinally the given parameters in params."""
        params.update(self.parameters)
        try:
            r = requests.get(self.server, params)
            return r
        except Exception as e:
            print("Error while sending http request: {0}".format(e))

    @abstractmethod
    def execute(self, dry = False):
        """This method executes the test and stores its WMSTestResult which is available by the method getResult. If the optional parameter dry is True, the test does not send requests."""
        pass
    
    def getResult(self):
        """Returns the test result of the last test execution."""
        return self.result
    
    # def getBox(self):
        # """Returns the current bounding box of the test."""
        # return self.parameters["bbox"]
    
    def __str__(self):
        """Generates a string representation of the WMSTest."""
        return self.server.__str__() + " " + self.parameters.__str__()


class RandomBoundingBoxWMSTest(WMSTest):
    """Generates a box inside the given bounding box with a random lower left corner and random width and heigth."""
    minwidth = 1.0
    minheight = 1.0
    maxfractionaldigits = 3
    # Internally, a point is represented as a pair of doubles and a box as pair of the lower left and the upper right points.
    box = Box()
    
    def __init__(self, server, layers, width, height, **kwargs):
        """Initiates a RandomBoundingBoxWMSTest with the given minimal witdth and height (defaults: 1.0). The default bounding box is [[-180.0, -90.0], [[180.0, 90.0]]. Optionally, the parameters minwidth, minheight, box and maxfractionaldigits can be altered."""
        super(RandomBoundingBoxWMSTest, self).__init__("RandomBbox", server, layers, width, height)
        if kwargs.__contains__("minwidth"):
            self.minwidth = kwargs["minwidth"]
        if kwargs.__contains__("minheight"):
            self.minheight = kwargs["minheight"]
        if kwargs.__contains__("maxfractionaldigits"):
            self.maxfractionaldigits = kwargs["maxfractionaldigits"]
        if kwargs.__contains__("box"):
            box = kwargs["box"]
            if not isinstance(box, Box):
                    raise Exception("The box parameter must be of type Box: " + box)
            if not super().spatialextent.contains(box):
                raise Exception("The box must be inside the spatial extent " + super().spatialextent + ".")
            self.box = box
        else:
            self.box = super().spatialextent.shuffle(self.minwidth, self.minheight, self.maxfractionaldigits)
        
    def setMaxFractionalDigits(self, maxfractionaldigits):
        """Sets the maximum number of fractional digits in any random number generated."""
        self.maxfractionaldigits = maxfractionaldigits

    def execute(self, dry = False):
        """This method executes the test once."""
        super().setBoundingBox(self.box)
        self.result = WMSTestResult([], [self.box]) if dry else WMSTestResult([super().request()], [self.box])


# # class WalkingBoxWMSTest(WMSTest):
    # # """Moves a given box by a random step width on the x and on the y axis."""
    # # minstepwidth = 1.0
    # # maxstepwidth = 10.0
    # # maxfractionaldigits = 0
    # # # Internally, a point is represented as a pair of doubles and a box as pair of the lower left and the upper right points.
    # # box = []
    
    # # def __init__(self, server, layers, width, height, **kwargs):
        # # """Initiates a RandomBoundingBoxWMSTest with the given minimal witdth and height (defaults: 1.0). The default bounding box is [[-180.0, -90.0], [[180.0, 90.0]]. Optionally, the parameters minstepwidth, maxstepwidth, box and maxfractionaldigits can be altered."""
        # # super(WalkingBoxWMSTest, self).__init__("WalkingBbox", server, layers, width, height)
        # # if kwargs.__contains__("minstepwidth"):
            # # self.minstepwidth = kwargs["minstepwidth"]
        # # if kwargs.__contains__("maxstepwidth"):
            # # self.maxstepwidth = kwargs["maxstepwidth"]
        # # if kwargs.__contains__("maxfractionaldigits"):
            # # self.maxfractionaldigits = kwargs["maxfractionaldigits"]
        # # if kwargs.__contains__("box"):
            # # box = kwargs["box"]
            # # if not isinstance(box, list) or not isinstance(box[0], list) or box[0].__len__() != 2 or not isinstance(box[1], list) or box[1].__len__() != 2:
                    # # raise Exception("The box must be a list of two pairs.")
            # # if box[0][0] < super().spatialextent[0][0] or box[1][0] > super().spatialextent[1][0] or box[0][1] < super().spatialextent[0][1] or box[1][1] > super().spatialextent[1][1]:
                # # raise Exception("The box must be inside the spatial extent " + super().spatialextent + ".")
            # # self.box = box

# # class ZoomingBoxWMSTest(WMSTest):
    # # """Generates a box inside the given bounding box with a random lower left corner and random width and heigth."""
    # # minwidth = 1.0
    # # minheight = 1.0
    # # maxfractionaldigits = 0
    # # # Internally, a point is represented as a pair of doubles and a box as pair of the lower left and the upper right points.
    # # box = []
    
    # # def __init__(self, server, layers, width, height, **kwargs):
        # # """Initiates a RandomBoundingBoxWMSTest with the given minimal witdth and height (defaults: 1.0). The default bounding box is [[-180.0, -90.0], [[180.0, 90.0]]."""
        # # super(RandomBoundingBoxWMSTest, self).__init__("RandomBbox", server, layers, width, height)
        # # if kwargs.__contains__("minwidth"):


#
# Functions
#

def output(f, text):
    """Output the given text. If the file f is given, write the text as new line into f. Otherwise print the text to the console."""
    if f:
        f.write(text + '\n')
        f.flush()
    else:
        print(text)

def progress(count, total, status=''):
    """Progress bar from https://gist.github.com/vladignatyev/06860ec2040cb497f0f3. Returns the count incremented by 1."""
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))
    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)
    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
    sys.stdout.flush()
    return count + 1


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
    parser.add_argument("--output-format", dest="outputformat", default=OUTPUT_FORMATS[0], choices=OUTPUT_FORMATS, help="format of the test result")
    parser.add_argument("--output-file", dest="outputfile", help="path to a file where the output is stored (if given, the console output is supressed")
    # parser.add_argument("--input-file", dest="inputfile", help="path to a file containing one box defined by [[lower_x,lower_y],[upper_x,upper_y]] per line")
    parser.add_argument("--width", type=int, required=True, help="width of the requested maps")
    parser.add_argument("--height", type=int, required=True, help="height of the requested maps")
    parser.add_argument("--dry-run", dest="dry", action='store_true', help="do not send, but only output the requests")
    parser.add_argument("layers", nargs='+', help="list of layer names to be tested against each other")
    parser.add_argument("--tests", nargs='+', default=TEST_CLASSES[0], choices=TEST_CLASSES)
    parser.add_argument("--count", type=int, default=1, help="positive number of test repetitions")

    args = parser.parse_args()
    # print(args)
    outputfile = args.outputfile
    if outputfile:
        if not path.exists(path.dirname(outputfile)):
            raise Exception("The output directory " + path.dirname(outputfile) + " does not exist.")
        else:
            outputfile = open(outputfile, "w+")
    width = args.width
    height = args.height
    verbosity = args.verbose
    layers = args.layers
    testclasses = args.tests
    count = args.count if args.count > 0 else 1

    if(verbosity): print("Initizing tests... ", end = '')
    wmsserver = WMSServer(args.host, args.port, args.path)
    tests = [[[] for t in testclasses] for c in range(count)]
    for c in range(count):
        for i in range(testclasses.__len__()):
            t = testclasses[i]
            if t == TEST_CLASSES[0]:
                tests[c][i] = [RandomBoundingBoxWMSTest(wmsserver, layers[0], width, height)]
                tests[c][i] += [RandomBoundingBoxWMSTest(wmsserver, l, width, height, box=tests[c][i][0].box) for l in layers[1:]]
    if(verbosity): print("done.")
    if(verbosity == 2): print(tests)

    if(verbosity): print("Testing... ")
    if args.dry == False:
        if(verbosity):
            progressbarMax = count*testclasses.__len__()*layers.__len__()
            progressbarCount = 0
            progressbarCount = progress(progressbarCount, progressbarMax)
        for c in range(count):
                for i in range(testclasses.__len__()):
                    for t in tests[c][i]:
                        t.execute()
                        if args.outputformat == "csv":
                            output(outputfile, t.getResult().getCSV())
                        else:
                            output(outputfile, t.getResult().getCSV())
                        if verbosity: progressbarCount = progress(progressbarCount, progressbarMax)
        if verbosity: print()
    else:
        if(verbosity): print("(Request sending skipped.)")
        if args.outputformat == "csv":
            for c in range(count):
                for i in range(testclasses.__len__()):
                    for t in tests[c][i]:
                        output(outputfile, t.__str__())
        elif args.outputformat == "bboxes":
            for c in range(count):
                for i in range(testclasses.__len__()):
                    output(outputfile, tests[c][i]   [0].box.__str__().replace(" ", ""))
    if outputfile:
        outputfile.close()
    if(verbosity): print("done.")
    
    
if __name__ == '__main__':
    main()