from typing import Any
from weakref import WeakValueDictionary

class RALFramework:
    def __init__(self):
        self._nodes = WeakValueDictionary()
        self._nodesByIndex = WeakValueDictionary()
        self._rememberedNodes = set()
        self._nodeIndexCounter = 0
        self._triples = set()

    def Node(self, *args):
        """
        Creates eather a data node or a constructed node depending on the arguments.
        (data: string, format: string): Creates a data node with the given data and format.
        (data: string): Creates a data node with the given data and the "text" format.
        (baseConnections: list): Creates a constructed node with the given base connections.
        """
        if type(args[0]) == str:
            if len(args) == 1:
                content = (args[0], "text")
            else:
                content = (args[0], args[1])
            isDataNode = True
        else:
            content = frozenset([tuple(x) for x in args[0]])
            for connection in content:
                assert len(connection) == 3
                for node in connection:
                    assert type(node) == _RALNode or node == 0
            isDataNode = False
        if content in self._nodes:
            return self._nodes[content]
        else:
            node = _RALNode(content, isDataNode, self)
            return node
    
    def getAllNodes(self):
        return [*self._nodes.values()]
    
    def clearAllNodes(self):
        for node in self.getAllNodes():
            node.forceDeletion()

    def _newNodeIndex(self):
        self._nodeIndexCounter += 1
        return self._nodeIndexCounter
    
    def search(self, triples = [], data = {}, constructed = {}):
        # Create the search modules
        dataBlock, constructedBlock, tripleBlock = data, constructed, triples
        knownParameters = {}
        searchModules = []
        for dataParam, (data, format) in dataBlock.items():
            if type(data) == str and type(format) == str:
                knownParameters[dataParam] = self.Node(data, format)
            else:
                searchModules.append(DataSearchModule(dataParam, data, format, self))
        for constructedParam, baseConnections in constructedBlock.items():
            exactNumberOfBaseConnections = True
            if len(baseConnections) > 0 and baseConnections[-1] == "+":
                baseConnections = baseConnections[:-1]
                exactNumberOfBaseConnections = False
            for i in range(len(baseConnections)):
                searchModules.append(ConstructedSearchModule(constructedParam, baseConnections, i, exactNumberOfBaseConnections, self))
        for subj, pred, obj in tripleBlock:
            searchModules.append(TripleSearchModule(subj, pred, obj, self))
        # Search for all possible parameter combinations
        yield from searchAllSearchModules(searchModules, knownParameters)
class _RALNode:
    def __init__(self, content, isDataNode, RALFramework):
        self.content = content
        self._isDataNode = isDataNode
        self._remembered = False
        self._RALFramework = RALFramework
        self._index = RALFramework._newNodeIndex()
        self._linkedTriples = set()
        RALFramework._nodes[content] = self
        RALFramework._nodesByIndex[self._index] = self
        if not isDataNode:
            triples = self._myTriples()
            for triple in triples:
                RALFramework._triples.add(triple)
                for i in range(3):
                    RALFramework._nodesByIndex[triple[i]]._linkedTriples.add(triple)
    def _myTriples(self):
        if not self._isDataNode:
            ret = set()
            for subj, pred, obj in self.content:
                ret.add((self._index if subj == 0 else subj._index, self._index if pred == 0 else pred._index, self._index if obj == 0 else obj._index, self._index))
            return ret
    @property
    def framework(self):
        return self._RALFramework
    @property
    def type(self):
        return "data" if self._isDataNode else "constructed"
    @property
    def connections(self):
        assert not self._isDataNode
        return self.content
    @property
    def data(self):
        assert self._isDataNode
        return self.content[0]
    @property
    def format(self):
        assert self._isDataNode
        return self.content[1]
    @property
    def remembered(self):
        return self._remembered
    @remembered.setter
    def remembered(self, value):
        if self._remembered:
            if not value:
                self._remembered = False
                self._RALFramework._rememberedNodes.remove(self)
        else:
            if value:
                self._remembered = True
                self._RALFramework._rememberedNodes.add(self)
    @property
    def isDeleted(self):
        return self._RALFramework is None
    def __del__(self):
        self.forceDeletion()
    def forceDeletion(self):
        if self._RALFramework is None:
            return
        self._RALFramework._nodes.pop(self.content)
        self._RALFramework._rememberedNodes.discard(self)
        self._RALFramework._nodesByIndex.pop(self._index)
        ralFramework = self._RALFramework
        self._RALFramework = None
        if not self._isDataNode:
            triples = self._myTriples()
            for triple in triples:
                ralFramework._triples.discard(triple)
                for i in range(3):
                    connectedNode = ralFramework._nodesByIndex.get(triple[i])
                    if connectedNode is not None:
                        connectedNode._linkedTriples.remove(triple)
                nodeUsingThisNode = ralFramework._nodesByIndex.get(triple[3])
                if nodeUsingThisNode is not None:
                    nodeUsingThisNode.forceDeletion()

def searchAllSearchModules(searchModules, knownParameters):
    """
    Return all filled parameter combinations for the given modules.
    """
    # If there are no modules yield the known parameters
    if len(searchModules) == 0:
        yield knownParameters
        return
    # Get the module with the smallest number of unknown parameters
    smallestUndefinednessIndex = None
    moduleWithSmallestNumberOfUnknownParameters = None
    for searchModule in searchModules:
        undefinednessIndex = searchModule.getUndefinednessIndex(knownParameters)
        if smallestUndefinednessIndex == None or undefinednessIndex < smallestUndefinednessIndex:
            smallestUndefinednessIndex = undefinednessIndex
            moduleWithSmallestNumberOfUnknownParameters = searchModule
    # Iterate through all possible values for the unknown parameters of the module
    for parameterValues in moduleWithSmallestNumberOfUnknownParameters.search(knownParameters):
        # Add the parameter values to the known parameters
        newKnownParameters = knownParameters | parameterValues
        # Recursively search for the remaining modules
        for newKnownParameters in searchAllSearchModules([searchModule for searchModule in searchModules if searchModule != moduleWithSmallestNumberOfUnknownParameters], newKnownParameters):
            yield newKnownParameters

class TripleSearchModule:
    def __init__(self, subj, pred, obj, framework):
        self.subj = subj
        self.pred = pred
        self.obj = obj
        self.framework = framework
        self.parameterNames = ({subj} if type(subj) == str else set()) | ({pred} if type(pred) == str else set()) | ({obj} if type(obj) == str else set())
    def getUndefinednessIndex(self, knownParameters):
        return len([parameter for parameter in self.parameterNames if parameter not in knownParameters])
    def search(self, knownParameters):
        subjValue = knownParameters.get(self.subj, None) if type(self.subj) == str else self.subj
        predValue = knownParameters.get(self.pred, None) if type(self.pred) == str else self.pred
        objValue = knownParameters.get(self.obj, None) if type(self.obj) == str else self.obj
        searchTriples = subjValue._linkedTriples if subjValue != None else predValue._linkedTriples if predValue != None else objValue._linkedTriples if objValue != None else self.framework._triples
        matchingTriples = [triple for triple in searchTriples if (subjValue == None or triple[0] == subjValue._index) and (predValue == None or triple[1] == predValue._index) and (objValue == None or triple[2] == objValue._index)]
        for matchingTriple in matchingTriples:
            yield {**({self.subj : self.framework._nodesByIndex[matchingTriple[0]]} if type(self.subj) == str else {}),
                   **({self.pred : self.framework._nodesByIndex[matchingTriple[1]]} if type(self.pred) == str else {}),
                   **({self.obj : self.framework._nodesByIndex[matchingTriple[2]]} if type(self.obj) == str else {})}
class ConstructedSearchModule:
    def __init__(self, param, baseConnections, connectionIndex, exactNumberOfBaseConnections, framework):
        self.framework = framework
        self.param = param
        self.baseConnections = baseConnections
        self.connectionIndex = connectionIndex
        self.exactNumberOfBaseConnections = exactNumberOfBaseConnections
        self.subj = baseConnections[connectionIndex][0]
        self.pred = baseConnections[connectionIndex][1]
        self.obj = baseConnections[connectionIndex][2]
        self.subj = self.subj if self.subj != 0 else param
        self.pred = self.pred if self.pred != 0 else param
        self.obj = self.obj if self.obj != 0 else param
        self.parameterNames = ({param} if type(param) == str else set()) | ({self.subj} if type(self.subj) == str else set()) | ({self.pred} if type(self.pred) == str else set()) | ({self.obj} if type(self.obj) == str else set())
    def getUndefinednessIndex(self, knownParameters):
        return len([parameter for parameter in self.parameterNames if parameter not in knownParameters])
    def search(self, knownParameters):
        subjValue = knownParameters.get(self.subj, None) if type(self.subj) == str else self.subj
        predValue = knownParameters.get(self.pred, None) if type(self.pred) == str else self.pred
        objValue = knownParameters.get(self.obj, None) if type(self.obj) == str else self.obj
        ownerValue = knownParameters.get(self.param, None) if type(self.param) == str else self.param
        searchTriples = ownerValue._linkedTriples if ownerValue != None else subjValue._linkedTriples if subjValue != None else predValue._linkedTriples if predValue != None else objValue._linkedTriples if objValue != None else self.framework._triples
        matchingTriples = [
            [self.framework._nodesByIndex[triple[0]], self.framework._nodesByIndex[triple[1]], self.framework._nodesByIndex[triple[2]], self.framework._nodesByIndex[triple[3]]]
            for triple in searchTriples if (subjValue == None or triple[0] == subjValue._index) and (predValue == None or triple[1] == predValue._index) and (objValue == None or triple[2] == objValue._index) and (ownerValue == None or triple[3] == ownerValue._index)]
        if len(matchingTriples) == 0:
            return
        # Create the set of already matched triples
        alreadyMatchedTriples = set()
        for i, matchingTriple in enumerate(self.baseConnections):
            if i != self.connectionIndex:
                subjValue = knownParameters.get(matchingTriple[0], None) if type(matchingTriple[0]) == str else matchingTriple[0]
                predValue = knownParameters.get(matchingTriple[1], None) if type(matchingTriple[1]) == str else matchingTriple[1]
                objValue = knownParameters.get(matchingTriple[2], None) if type(matchingTriple[2]) == str else matchingTriple[2]
                if not None in [subjValue, predValue, objValue]:
                    alreadyMatchedTriples.add((subjValue, predValue, objValue))
        # Iterate through the matching triples
        for matchingTriple in matchingTriples:
            # If the owner got a new value, check if there is an exact number of base connections
            if ownerValue == None and self.exactNumberOfBaseConnections:
                if len(self.baseConnections) != len(matchingTriple[3].content):
                    continue
            # Check if the triple is already matched
            if (matchingTriple[0], matchingTriple[1], matchingTriple[2]) in alreadyMatchedTriples:
                continue
            yield {**({self.subj : matchingTriple[0]} if type(self.subj) == str else {}),
                   **({self.pred : matchingTriple[1]} if type(self.pred) == str else {}),
                   **({self.obj : matchingTriple[2]} if type(self.obj) == str else {}),
                   **({self.param : matchingTriple[3]} if type(self.param) == str else {})}
class DataSearchModule:
    def __init__(self, param, data, format, framework):
        self.framework = framework
        self.param = param
        self.data = data
        self.format = format
        self.parameterNames = ({param} if type(param) == str else set()) | ({data[0]} if type(data) == list else set()) | ({format[0]} if type(format) == list else set())
    def getUndefinednessIndex(self, knownParameters):
        return len([parameter for parameter in self.parameterNames if parameter not in knownParameters])
    def search(self, knownParameters):
        paramValue = knownParameters.get(self.param, None) if type(self.param) == str else self.param
        dataValue = knownParameters.get(self.data[0], None) if type(self.data) == list else self.data
        formatValue = knownParameters.get(self.format[0], None) if type(self.format) == list else self.format
        matchingAbstractions = [(node, node.data, node.format) for node in self.framework._nodes.values() if node._isDataNode and (paramValue == None or node == paramValue) and (dataValue == None or node.data == dataValue) and (formatValue == None or node.format == formatValue)]
        for matchingAbstraction in matchingAbstractions:
            yield {**({self.param : matchingAbstraction[0]} if type(self.param) == str else {}),
                   **({self.data[0] : matchingAbstraction[1]} if type(self.data) == list else {}),
                   **({self.format[0] : matchingAbstraction[2]} if type(self.format) == list else {})}

