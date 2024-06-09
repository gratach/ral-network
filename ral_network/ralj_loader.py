# Load and save data from RALJ files into the neo4j database.
# RALJ format documentation:
#   https://github.com/gratach/thoughts/blob/master/topics/data/graph/reduced-abstraction-layer-json.md

import json

def loadRALJFile(file_path, RALFramework):
    with open(file_path, "r") as file:
        data = json.load(file)
    return loadRALJData(data, RALFramework)

def loadRALJData(data, RALFramework):
    assert type(data) == list and len(data) < 5
    dataConceptBlock = data[0] if len(data) > 0 else {}
    constructedConceptBlock = data[1] if len(data) > 1 else {}
    directAbstractionBlock = data[2] if len(data) > 2 else {}
    inverseDirectAbstractionBlock = data[3] if len(data) > 3 else {}
    abstractionIDByJsonNodeID = {}
    relatingAbstractionsByJsonNodeID = {}
    uncheckedJsonNodeIDs = set(constructedConceptBlock.keys()).union(set(directAbstractionBlock.keys())).union(set(inverseDirectAbstractionBlock.keys()))
    loadedJsonNodeIDs = set()
    # Load all direct data abstractions
    for format, dataConcepts in dataConceptBlock.items():
        for data, jsonNodeID in dataConcepts.items():
            abstractionIDByJsonNodeID[jsonNodeID] = RALFramework.Node(data, format)
            loadedJsonNodeIDs.add(jsonNodeID)
    # Load all constructed abstractions
    while len(uncheckedJsonNodeIDs) > 0:
        jsonNodeID = uncheckedJsonNodeIDs.pop()
        if jsonNodeID in directAbstractionBlock:
            # The abstraction is a direct abstraction
            innerJsonNodeID = directAbstractionBlock[jsonNodeID]
            if innerJsonNodeID not in loadedJsonNodeIDs:
                if innerJsonNodeID not in relatingAbstractionsByJsonNodeID:
                    relatingAbstractionsByJsonNodeID[innerJsonNodeID] = set()
                relatingAbstractionsByJsonNodeID[innerJsonNodeID].add(jsonNodeID)
                continue
            abstractionIDByJsonNodeID[jsonNodeID] = RALFramework.DirectAbstraction(abstractionIDByJsonNodeID[innerJsonNodeID])
            loadedJsonNodeIDs.add(jsonNodeID)
        elif jsonNodeID in inverseDirectAbstractionBlock:
            # The abstraction is a direct abstraction
            innerJsonNodeID = inverseDirectAbstractionBlock[jsonNodeID]
            if innerJsonNodeID not in loadedJsonNodeIDs:
                if innerJsonNodeID not in relatingAbstractionsByJsonNodeID:
                    relatingAbstractionsByJsonNodeID[innerJsonNodeID] = set()
                relatingAbstractionsByJsonNodeID[innerJsonNodeID].add(jsonNodeID)
                continue
            abstractionIDByJsonNodeID[jsonNodeID] = RALFramework.InverseDirectAbstraction(abstractionIDByJsonNodeID[innerJsonNodeID])
            loadedJsonNodeIDs.add(jsonNodeID)
        else:
            # The abstraction is a constructed abstraction
            baseConnections = constructedConceptBlock[jsonNodeID]
            allConnectedJsonNodeIDsLoaded = True
            for connection in baseConnections:
                for connectedJsonNodeID in connection:
                    if connectedJsonNodeID != 0 and connectedJsonNodeID not in loadedJsonNodeIDs:
                        allConnectedJsonNodeIDsLoaded = False
                        if connectedJsonNodeID not in relatingAbstractionsByJsonNodeID:
                            relatingAbstractionsByJsonNodeID[connectedJsonNodeID] = set()
                        relatingAbstractionsByJsonNodeID[connectedJsonNodeID].add(jsonNodeID)
                        break
                if not allConnectedJsonNodeIDsLoaded:
                    break
            if allConnectedJsonNodeIDsLoaded:
                # Load the abstraction
                baseConnections = [[0 if y == 0 else abstractionIDByJsonNodeID[y] for y in x] for x in baseConnections]
                abstractionIDByJsonNodeID[jsonNodeID] = RALFramework.Node(baseConnections)
                loadedJsonNodeIDs.add(jsonNodeID)
            else:
                continue
        # The abstraction is loaded
        if jsonNodeID in relatingAbstractionsByJsonNodeID:
            for relatingJsonNodeID in relatingAbstractionsByJsonNodeID[jsonNodeID]:
                if not relatingJsonNodeID in loadedJsonNodeIDs:
                    uncheckedJsonNodeIDs.add(relatingJsonNodeID)
            del relatingAbstractionsByJsonNodeID[jsonNodeID]
    return abstractionIDByJsonNodeID

def saveRALJFile(abstractions, file_path, RALFramework):
    with open(file_path, "w") as file:
        data = saveRALJData(abstractions, RALFramework)
        json.dump(data, file)

def saveRALJData(abstractions, RALFramework):
    jsonNodeIDByAbstractionID = {}
    relatingAbstractionsByAbstractionID = {}
    uncheckedAbstractions = set(abstractions)
    savedAbstractions = set()
    jsonNodeIndex = 1
    dataConceptBlock = {}
    constructedConceptBlock = {}
    directAbstractionBlock = {}
    inverseDirectAbstractionBlock = {}
    while len(uncheckedAbstractions) > 0:
        abstraction = uncheckedAbstractions.pop()
        abstractionType = abstraction.type
        if abstractionType == "data":
            data, format = abstraction.content
            if format not in dataConceptBlock:
                dataConceptBlock[format] = {}
            if data not in dataConceptBlock[format]:
                jsonNodeName = str(jsonNodeIndex)
                jsonNodeIndex += 1
                dataConceptBlock[format][data] = jsonNodeName
                jsonNodeIDByAbstractionID[abstraction] = jsonNodeName
        elif abstractionType == "constructed":
            # Check if all connected abstractions are saved
            semanticConnections = abstraction.connections
            allConnectedAbstractionsSaved = True
            for connection in semanticConnections:
                for connectedAbstraction in connection:
                    if connectedAbstraction != 0 and connectedAbstraction not in savedAbstractions:
                        uncheckedAbstractions.add(connectedAbstraction)
                        allConnectedAbstractionsSaved = False
                        # Add the relating abstraction
                        if connectedAbstraction not in relatingAbstractionsByAbstractionID:
                            relatingAbstractionsByAbstractionID[connectedAbstraction] = set()
                        relatingAbstractionsByAbstractionID[connectedAbstraction].add(abstraction)
                        break
                if not allConnectedAbstractionsSaved:
                    break
            if allConnectedAbstractionsSaved:
                # Save the abstraction
                jsonNodeName = str(jsonNodeIndex)
                jsonNodeIndex += 1
                jsonNodeIDByAbstractionID[abstraction] = jsonNodeName
                jsonSemanticConnections = [[0 if y == 0 else jsonNodeIDByAbstractionID[y] for y in x] for x in semanticConnections]
                constructedConceptBlock[jsonNodeName] = jsonSemanticConnections
            else:
                continue
        elif abstractionType == "DirectAbstraction":
            innerAbstraction = RALFramework.getAbstractionContent(abstraction)
            if innerAbstraction not in savedAbstractions:
                uncheckedAbstractions.add(innerAbstraction)
                # Add the relating abstraction
                if innerAbstraction not in relatingAbstractionsByAbstractionID:
                    relatingAbstractionsByAbstractionID[innerAbstraction] = set()
                relatingAbstractionsByAbstractionID[innerAbstraction].add(abstraction)
                continue
            else:
                # Save the direct abstraction
                jsonNodeName = str(jsonNodeIndex)
                jsonNodeIndex += 1
                jsonNodeIDByAbstractionID[abstraction] = jsonNodeName
                directAbstractionBlock[jsonNodeName] = jsonNodeIDByAbstractionID[innerAbstraction]
        elif abstractionType == "InverseDirectAbstraction":
            innerAbstraction = RALFramework.getAbstractionContent(abstraction)
            if innerAbstraction not in savedAbstractions:
                uncheckedAbstractions.add(innerAbstraction)
                # Add the relating abstraction
                if innerAbstraction not in relatingAbstractionsByAbstractionID:
                    relatingAbstractionsByAbstractionID[innerAbstraction] = set()
                relatingAbstractionsByAbstractionID[innerAbstraction].add(abstraction)
                continue
            else:
                # Save the inverse direct abstraction
                jsonNodeName = str(jsonNodeIndex)
                jsonNodeIndex += 1
                jsonNodeIDByAbstractionID[abstraction] = jsonNodeName
                inverseDirectAbstractionBlock[jsonNodeName] = jsonNodeIDByAbstractionID[innerAbstraction]
        # The abstraction is saved
        savedAbstractions.add(abstraction)
        # Add the relating abstractions
        if abstraction in relatingAbstractionsByAbstractionID:
            for relatingAbstraction in relatingAbstractionsByAbstractionID[abstraction]:
                if not relatingAbstraction in savedAbstractions:
                    uncheckedAbstractions.add(relatingAbstraction)
            del relatingAbstractionsByAbstractionID[abstraction]
    return [dataConceptBlock, constructedConceptBlock, *([directAbstractionBlock, inverseDirectAbstractionBlock] if len(directAbstractionBlock) > 0 or len(inverseDirectAbstractionBlock) > 0 else [])]