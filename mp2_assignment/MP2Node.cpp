/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable;
	this->memberNode->addr = *address;
	this->currentTransaction = 0;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	curMemList.push_back(Node(this->memberNode->addr));
	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
	ring = curMemList;

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	stabilizationProtocol();
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	vector<Node> nodes = findNodes(key);

	//transID::fromAddr::CREATE::key::value::ReplicaType

	for (int i = 0; i < 3; i++) {
		Message message = Message(currentTransaction, this->memberNode->addr, CREATE, key, value,
								  static_cast<ReplicaType>(i));
		this->emulNet->ENsend(&this->memberNode->addr, &nodes[i].nodeAddress, message.toString());

	}
	outgoingMessages.emplace(currentTransaction, Message(currentTransaction, this->memberNode->addr, CREATE, key, value));
	sucessedTransactions.emplace(currentTransaction, 0);
	failedTransactions.emplace(currentTransaction, 0);
	currentTransaction++;
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	vector<Node> nodes = findNodes(key);

	// transID::fromAddr::READ::key
	for (int i = 0; i < 3; i++) {
		Message message = Message(currentTransaction, this->memberNode->addr, READ, key);
		this->emulNet->ENsend(&this->memberNode->addr, &nodes[i].nodeAddress, message.toString());
	}
	outgoingMessages.emplace(currentTransaction, Message(currentTransaction, this->memberNode->addr, READ, key));
	sucessedTransactions.emplace(currentTransaction, 0);
	failedTransactions.emplace(currentTransaction, 0);
	currentTransaction++;
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value) {
	vector<Node> nodes = findNodes(key);

	// transID::fromAddr::UPDATE::key::value::ReplicaType

	for (int i = 0; i < 3; i++) {
		Message message = Message(currentTransaction, this->memberNode->addr, UPDATE, key, value, static_cast<ReplicaType>(i));
		this->emulNet->ENsend(&this->memberNode->addr, &nodes[i].nodeAddress, message.toString());
	}
	outgoingMessages.emplace(currentTransaction, Message(currentTransaction, this->memberNode->addr, UPDATE, key, value));
	sucessedTransactions.emplace(currentTransaction, 0);
	failedTransactions.emplace(currentTransaction, 0);
	currentTransaction++;
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	vector<Node> nodes = findNodes(key);

	// transID::fromAddr::DELETE::key

	for (int i = 0; i < 3; i++) {
		Message message = Message(currentTransaction, this->memberNode->addr, DELETE, key);
		this->emulNet->ENsend(&this->memberNode->addr, &nodes[i].nodeAddress, message.toString());
	}
	outgoingMessages.emplace(currentTransaction, Message(currentTransaction, this->memberNode->addr, DELETE, key));
	sucessedTransactions.emplace(currentTransaction, 0);
	failedTransactions.emplace(currentTransaction, 0);
	currentTransaction++;
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	// Insert key, value, replicaType into the hash table
	return ht->create(key, value);
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	return  ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	// Update key in local hash table and return true or false
	return  ht->update(key, value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	// Delete the key from the local hash table
	return  ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
#ifdef DEBUGLOG2
	std::cout << "Check messages on node " << this->memberNode->addr.getAddress() << '\n';
#endif
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string value(data, data + size);

		Message message(value);

		switch (message.type) {
			case CREATE: {
#ifdef DEBUGLOG2
				std::cout << "Received create on node " << this->memberNode->addr.getAddress() << '\n';
#endif
				bool result = createKeyValue(message.key, message.value, message.replica);
				if (message.transID != -1) {
					if (result) {
						log->logCreateSuccess(&this->memberNode->addr, false, message.transID, message.key,
											  message.value);
					} else {
						log->logCreateFail(&this->memberNode->addr, false, message.transID, message.key, message.value);
					}
				}
				Message answer = Message(message.transID, this->memberNode->addr, REPLY, result);
				this->emulNet->ENsend(&this->memberNode->addr, &message.fromAddr, answer.toString());
#ifdef DEBUGLOG2
				std::cout << "Finished create on node " << this->memberNode->addr.getAddress() << '\n';
#endif
				break;
			}
			case READ: {
#ifdef DEBUGLOG2
				std::cout << "Received read on node " << this->memberNode->addr.getAddress() << '\n';
#endif
				string result = readKey(message.key);
				if (message.transID != -1) {
					if (result.length() != 0) {
						log->logReadSuccess(&this->memberNode->addr, false, message.transID, message.key, result);
					} else {
						log->logReadFail(&this->memberNode->addr, false, message.transID, message.key);
					}
				}
				Message answer = Message(message.transID, this->memberNode->addr, result);
				this->emulNet->ENsend(&this->memberNode->addr, &message.fromAddr, answer.toString());
#ifdef DEBUGLOG2
				std::cout << "Finished read on node " << this->memberNode->addr.getAddress() << '\n';
#endif
				break;
			}
			case UPDATE: {
#ifdef DEBUGLOG2
				std::cout << "Received update on node " << this->memberNode->addr.getAddress() << '\n';
#endif
				bool result = updateKeyValue(message.key, message.value, message.replica);
				if (message.transID != -1) {
					if (result) {
						log->logUpdateSuccess(&this->memberNode->addr, false, message.transID, message.key,
											  message.value);
					} else {
						log->logUpdateFail(&this->memberNode->addr, false, message.transID, message.key, message.value);
					}
				}
				Message answer = Message(message.transID, this->memberNode->addr, REPLY, result);
				this->emulNet->ENsend(&this->memberNode->addr, &message.fromAddr, answer.toString());
#ifdef DEBUGLOG2
				std::cout << "Finished update on node " << this->memberNode->addr.getAddress() << '\n';
#endif
				break;
			}
			case DELETE: {
#ifdef DEBUGLOG2
				std::cout << "Received delete on node " << this->memberNode->addr.getAddress() << '\n';
#endif
				bool result = deletekey(message.key);
				if (message.transID != -1) {
					if (result) {
						log->logDeleteSuccess(&this->memberNode->addr, false, message.transID, message.key);
					} else {
						log->logDeleteFail(&this->memberNode->addr, false, message.transID, message.key);
					}
				}
				Message answer = Message(message.transID, this->memberNode->addr, REPLY, result);
				this->emulNet->ENsend(&this->memberNode->addr, &message.fromAddr, answer.toString());
#ifdef DEBUGLOG2
				std::cout << "Finished delete on node " << this->memberNode->addr.getAddress() << '\n';
#endif
				break;
			}
			case REPLY: {
#ifdef DEBUGLOG2
				std::cout << "Received reply on node " << this->memberNode->addr.getAddress() << '\n';
#endif
				if (message.transID == -1) {
					break;
				}
				if (message.success) {
					sucessedTransactions.at(message.transID) += 1;
				} else {
					failedTransactions.at(message.transID) += 1;
				}
				Message outMessage = outgoingMessages.find(message.transID)->second;
				if (message.success && sucessedTransactions.at(message.transID) == 2) {
					switch (outMessage.type) {
						case CREATE:
							log->logCreateSuccess(&this->memberNode->addr, true, outMessage.transID, outMessage.key,
												  outMessage.value);
							break;
						case UPDATE:
							log->logUpdateSuccess(&this->memberNode->addr, true, outMessage.transID, outMessage.key,
												  outMessage.value);
							break;
						case DELETE:
							log->logDeleteSuccess(&this->memberNode->addr, true, outMessage.transID, outMessage.key);
							break;
						case READ:
							break;
						case REPLY:
							break;
						case READREPLY:
							break;
					}
				} else if (!message.success && failedTransactions.at(message.transID) == 2) {
					switch (outMessage.type) {
						case CREATE:
							log->logCreateFail(&this->memberNode->addr, true, outMessage.transID, outMessage.key,
											   outMessage.value);
							break;
						case UPDATE:
							log->logUpdateFail(&this->memberNode->addr, true, outMessage.transID, outMessage.key,
											   outMessage.value);
							break;
						case DELETE:
							log->logDeleteFail(&this->memberNode->addr, true, outMessage.transID, outMessage.key);
							break;
						case READ:
							break;
						case REPLY:
							break;
						case READREPLY:
							break;
					}
				}
#ifdef DEBUGLOG2
				std::cout << "Finished reply on node " << this->memberNode->addr.getAddress() << '\n';
#endif
				break;
			}
			case READREPLY: {
#ifdef DEBUGLOG2
				std::cout << "Received readreply on node " << this->memberNode->addr.getAddress() << '\n';
#endif
				if (message.transID == -1) {
					break;
				}
				if (message.value.length() != 0) {
					sucessedTransactions.at(message.transID) += 1;
				} else {
					failedTransactions.at(message.transID) += 1;
				}
				Message outMessage = outgoingMessages.find(message.transID)->second;
				if (message.value.length() != 0 && sucessedTransactions.at(message.transID) == 2) {
					log->logReadSuccess(&this->memberNode->addr, true, outMessage.transID, outMessage.key, message.value);
				} else if (message.value.length() == 0 && failedTransactions.at(message.transID) == 2) {
					log->logReadFail(&this->memberNode->addr, true, outMessage.transID, outMessage.key);
				}
#ifdef DEBUGLOG2
				std::cout << "Finished readreaply on node " << this->memberNode->addr.getAddress() << '\n';
#endif
				break;
			}
		}
	}
#ifdef DEBUGLOG2
	std::cout << "Finish checking messages on node " << this->memberNode->addr.getAddress() << '\n';
#endif
	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
	for (auto& trans : sucessedTransactions) {
		if (trans.second == 1 && failedTransactions.at(trans.first) < 2) {
			Message outMessage = outgoingMessages.find(trans.first)->second;
			switch (outMessage.type) {
				case CREATE:
					log->logCreateFail(&this->memberNode->addr, true, outMessage.transID, outMessage.key, outMessage.value);
					break;
				case UPDATE:
					log->logUpdateFail(&this->memberNode->addr, true, outMessage.transID, outMessage.key, outMessage.value);
					break;
				case DELETE:
					log->logDeleteFail(&this->memberNode->addr, true, outMessage.transID, outMessage.key);
					break;
				case READ:
					log->logReadFail(&this->memberNode->addr, true, outMessage.transID, outMessage.key);
					break;
				case REPLY:
					break;
				case READREPLY:
					break;
			}
			failedTransactions.erase(trans.first);
			sucessedTransactions.erase(trans.first);
		}
	}
}

vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	return findNodes(pos);
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(size_t pos) {
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
#ifdef DEBUGLOG2
	std::cout << "Nodes ";
	for (Node node : addr_vec) {
		std::cout << node.nodeAddress.getAddress() <<  " with hash " << node.nodeHashCode << " ";
	}
	std::cout << '\n';
#endif
	return addr_vec;

}

vector<Node> MP2Node::findOldNodes(string key) {
	size_t key_hash = hashFunction(key);
	return findOldNodes(key_hash);
}

vector<Node> MP2Node::findOldNodes(size_t pos) {
	vector<Node> result;
	for (Node node : haveReplicasOf) {
		if (node.nodeHashCode >= pos) {
			result.emplace_back(node);
		}
	}
	result.emplace_back(Node(this->memberNode->addr));
	for (int i = 1; i <= 3 - result.size(); i++) {
		result.emplace_back(hasMyReplicas.at(i));
	}
#ifdef DEBUGLOG2
	std::cout << "Stabilaze old nodes ";
	for (Node node : result) {
		std::cout << node.nodeAddress.getAddress() <<  " with hash " << node.nodeHashCode << " ";
	}
	std::cout << '\n';
#endif
	return  result;
}

vector<Node> MP2Node::minus(vector<Node> from, vector<Node> to) {
	vector<Node> result;
	for (Node node : from) {
		bool contains = false;
		for (Node toNode : to) {
			if (toNode.nodeHashCode == node.nodeHashCode) {
				contains = true;
				break;
			}
		}
		if (!contains) {
			result.emplace_back(node);
		}
	}
#ifdef DEBUGLOG2
	std::cout << "Minus ";
	for (Node node : result) {
		std::cout << node.nodeAddress.getAddress() <<  " with hash " << node.nodeHashCode << " ";
	}
	std::cout << '\n';
#endif
	return result;
}

vector<Node> MP2Node::filterOnRing(vector<Node> nodes) {
	return minus(nodes, minus(nodes, ring));
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
	if (hasMyReplicas.size() == 0 && haveReplicasOf.size() == 0) {
#ifdef DEBUGLOG2
		std::cout << "Init sucessors and predecessors for node " << this->memberNode->addr.getAddress() << '\n';
#endif
		hasMyReplicas = findNodes(hashFunction(this->memberNode->addr.addr));
		haveReplicasOf = findPredecessors();
#ifdef DEBUGLOG2
		std::cout << "Sucessors and predecessors for node " << this->memberNode->addr.getAddress() << " found!" << '\n';
#endif
		return;
	}
#ifdef DEBUGLOG2
	std::cout << "Start stabiliztion on node " << this->memberNode->addr.getAddress() << '\n';
#endif
	for (auto& entry: ht->hashTable) {
		vector<Node> replicas = findNodes(entry.first);
		vector<Node> currentNodes = findOldNodes(entry.first);

		vector<Node> addNodes = minus(replicas, currentNodes);
		vector<Node> removeNodes = minus(currentNodes, replicas);

		for (Node node : removeNodes) {
#ifdef DEBUGLOG2
			std::cout << "Stabilaze create key " << entry.first << " on node " << node.nodeAddress.getAddress() <<  " with hash "
				<< node.nodeHashCode << "\n";
#endif
			Message message = Message(-1, this->memberNode->addr, DELETE, entry.first);
			this->emulNet->ENsend(&this->memberNode->addr, &node.nodeAddress, message.toString());
		}

		for (Node node : addNodes) {
#ifdef DEBUGLOG2
			std::cout << "Stabilaze remove key " << entry.first << " on node " << node.nodeAddress.getAddress() <<  " with hash "
				<< node.nodeHashCode << "\n";
#endif
			Message message = Message(-1, this->memberNode->addr, CREATE, entry.first, entry.second, PRIMARY);
			this->emulNet->ENsend(&this->memberNode->addr, &node.nodeAddress, message.toString());
		}
	}

	hasMyReplicas = findNodes(hashFunction(this->memberNode->addr.addr));
	haveReplicasOf = findPredecessors();
#ifdef DEBUGLOG2
	std::cout << "Stabiliztion on node " << this->memberNode->addr.getAddress() <<  " finished!" << '\n';
#endif
}

bool MP2Node::onRing(Node node) {
	for (Node ringNode : ring) {
		if (ringNode.nodeHashCode == node.nodeHashCode) {
			return true;
		}
	}
	return false;
}

Node MP2Node::findPrimary(string key) {
	return findNodes(key).front();
}

vector<Node> MP2Node::findSuccessors() {
	vector<Node> addr_vec;
	size_t pos = hashFunction(this->memberNode->addr.addr);
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos == ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos < addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

vector<Node> MP2Node::findPredecessors() {
	vector<Node> addr_vec;
	size_t pos = hashFunction(this->memberNode->addr.addr);
#ifdef DEBUGLOG2
	std::cout << "Search predecessors for " << this->memberNode->addr.getAddress() <<  " with hash " << pos << '\n';
#endif
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos == ring.at(0).getHashCode()) {
			addr_vec.emplace_back(ring.at(ring.size()-1));
			addr_vec.emplace_back(ring.at(ring.size()-2));
		} else if (pos == ring.at(1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(ring.size()-1));
		} else {
			// go through the ring until pos <= node
			for (int i = ring.size() - 1; i > 0; i--){
				Node addr = ring.at(i);
				if (pos > addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i - 1) % ring.size()));
					break;
				}
			}
		}
	}
#ifdef DEBUGLOG2
	for (Node node : addr_vec) {
		std::cout << node.nodeAddress.getAddress() <<  " with hash " << node.nodeHashCode << " ";
	}
	std::cout << '\n';
#endif
	return addr_vec;
}
