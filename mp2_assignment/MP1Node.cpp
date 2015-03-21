/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
    this->deleted = vector<int>();
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG22
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG22
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG22
    static char s[1024];
#endif

    if ( 0 == strcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG22
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG2
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
//    Address address;
//    while (!memberNode->memberList.empty()) {
//        MemberListEntry e = memberNode->memberList[memberNode->memberList.size() - 1];
//        memcpy(&address.addr, &e.id, sizeof(int));
//        memcpy(&address.addr[4], &e.port, sizeof(short));
//        log->logNodeRemove(&memberNode->addr, &address);
//        memberNode->memberList.pop_back();
//    }
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */

    Address address;
    long heartbeat;
    MessageHdr msg;
    memcpy((char*) &msg, data, sizeof(MessageHdr));
    memcpy((char*) &address, data + sizeof(MessageHdr), sizeof(address));
    memcpy((char*) &heartbeat, data + sizeof(MessageHdr) + sizeof(address), sizeof(long));
    int id = *(int*) &address.addr;
    short port = *(short*) &address.addr[4];

    switch (msg.msgType) {
        case JOINREQ: {
#ifdef DEBUGLOG2
            log->LOG(&memberNode->addr, "Received joinreq");
            char buffer[1024];
            log->LOG(&memberNode->addr, "Received joinreq id %d port %d", id, port);
            sprintAddress("Send response %d.%d.%d.%d:%d\n", buffer, &address);
            log->LOG(&memberNode->addr, buffer);
#endif
            memberNode->memberList.push_back(MemberListEntry(id, port, heartbeat, par->getcurrtime()));
            log->logNodeAdd(&memberNode->addr, &address);
            //send JOINREP
            size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr) + sizeof(long);
            char * answer = (char *) malloc(msgsize * sizeof(char));
            MessageHdr messageHdr;
            messageHdr.msgType = JOINREP;
            memcpy(answer, &messageHdr, sizeof(MessageHdr));
            memcpy(answer + sizeof(MessageHdr), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
            memcpy(answer + sizeof(MessageHdr) + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
            emulNet->ENsend(&memberNode->addr, &address, answer, msgsize);
            return true;
        } case JOINREP: {
#ifdef DEBUGLOG2
            log->LOG(&memberNode->addr, "Received joinrep");
            char buffer[1024];
            log->LOG(&memberNode->addr, "Received joinrep id %d port %d", id, port);
            sprintAddress("Receive joinrep %d.%d.%d.%d:%d\n", buffer, &address);
            log->LOG(&memberNode->addr, buffer);
#endif
            memberNode->inGroup = true;
            memberNode->memberList.push_back(MemberListEntry(id, port, heartbeat, par->getcurrtime()));
            log->logNodeAdd(&memberNode->addr, &address);
            return true;
        } case HEARTBEAT: {
            if (!memberNode->inGroup) {
#ifdef DEBUGLOG2
                log->LOG(&memberNode->addr, "Received heartbeat before joinrep");
#endif
                return false;
            }
            size_t entry_size = sizeof(int) + sizeof(short) + sizeof(long);
            size_t offset = sizeof(MessageHdr) + sizeof(memberNode->addr) + sizeof(long);
            size_t count = (size - offset) / entry_size;

            vector<MemberListEntry> table;
            for (size_t i = 0; i < count; i++) {
                Address addr = *(Address *) (data + offset);
                offset += sizeof(Address);
                int id = *(int *) &addr.addr;
                short port = *(short *) &addr.addr[4];
                long heartbeat = *(long *) (data + offset);
                offset += sizeof(long);
                table.push_back(MemberListEntry(id, port, heartbeat, par->getcurrtime()));
            }

            logMemberList(&memberNode->memberList, &memberNode->addr);
            logMemberList(&table, &address);

            mergeTable(&table);
            return true;
        } default:
#ifdef DEBUGLOG2
            log->LOG(&memberNode->addr, "Unknown message type");
#endif
            return false;
    }
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
#ifdef DEBUGLOG2
    log->LOG(&memberNode->addr, "Cleanups");
    logMemberList(&memberNode->memberList, &memberNode->addr);
#endif

    //Check nodes status
    for (int i =0; i < memberNode->memberList.size();) {
        MemberListEntry entry = memberNode->memberList[i];
        Address removed;
        memcpy(removed.addr, &entry.id, sizeof(int));
        memcpy(removed.addr + 4, &entry.port, sizeof(short));

        if (par->getcurrtime() - entry.timestamp > 10 && isDeleted(entry.id)) {
#ifdef DEBUGLOG2
            log->LOG(&memberNode->addr, "Remove node %d port %d", entry.id, entry.port);
#endif
            log->logNodeRemove(&memberNode->addr, &removed);

            memberNode->memberList.erase(memberNode->memberList.begin() + i);
            continue;
        } else if (par->getcurrtime() - entry.timestamp > 5 && !isDeleted(entry.id)) {
            deleted.push_back(entry.id);
#ifdef DEBUGLOG2
            log->LOG(&memberNode->addr, "Mark as deleted node %d port %d", entry.id, entry.port);
#endif
        }
        i++;
    }

    memberNode->heartbeat++;
    //Send heartbeats
    size_t entry_size = sizeof(int) + sizeof(short) + sizeof(long);
    size_t message_size = entry_size * (memberNode->memberList.size() + 1) + sizeof(MessageHdr)
            + sizeof(memberNode->addr) + sizeof(long);
#ifdef DEBUGLOG2
    log->LOG(&memberNode->addr, "DeletedSize %d", deleted.size());
    log->LOG(&memberNode->addr, "MessageSize %d", message_size);
#endif
    char *message = (char *) malloc(message_size);
    MessageHdr messageHdr;
    messageHdr.msgType = HEARTBEAT;
    memcpy(message, &messageHdr, sizeof(MessageHdr));
    size_t offset = sizeof(MessageHdr);
    memcpy(message + offset, &memberNode->addr, sizeof(memberNode->addr));
    offset += sizeof(memberNode->addr);
    memcpy(message + offset, &memberNode->heartbeat, sizeof(long));
    offset += sizeof(long);
    for (int i = 0; i < memberNode->memberList.size(); i++) {
        MemberListEntry entry = memberNode->memberList[i];
        memcpy(message + offset, &entry.id, sizeof(int));
        offset += sizeof(int);
        memcpy(message + offset, &entry.port, sizeof(short));
        offset += sizeof(short);
        memcpy(message + offset, &entry.heartbeat, sizeof(long));
        offset += sizeof(long);
    }

    //Myself
    memcpy(message + offset, &memberNode->addr.addr, sizeof(int));
    offset += sizeof(int);
    memcpy(message + offset, &memberNode->addr.addr[4], sizeof(short));
    offset += sizeof(short);
    memcpy(message + offset, &memberNode->heartbeat, sizeof(long));

    Address address;
    for (int i = 0; i < memberNode->memberList.size(); i++) {

        memcpy(&address.addr, &memberNode->memberList[i].id, sizeof(int));
        memcpy(&address.addr[4], &memberNode->memberList[i].port, sizeof(short));
        emulNet->ENsend(&memberNode->addr, &address, message, message_size);

#ifdef DEBUGLOG2
        char buffer[1024];
        log->LOG(&memberNode->addr, "Send id %d port %d", memberNode->memberList[i].id, memberNode->memberList[i].port);
        sprintAddress("Send table %d.%d.%d.%d:%d", buffer, &address);
        log->LOG(&memberNode->addr, buffer);
        log->LOG(&memberNode->addr, "Send heartbeat\n");
#endif
    }
    free(message);
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

bool MP1Node::isDeleted(int id) {
    std::vector<int>::iterator position = std::find(deleted.begin(), deleted.end(), id);
    return  position != deleted.end();
}

void MP1Node::removeFromDeleted(int id) {
    std::vector<int>::iterator position = std::find(deleted.begin(), deleted.end(), id);
    if (position != deleted.end()) {
        deleted.erase(position);
    }
}

void MP1Node::logMemberList(vector<MemberListEntry>* list, Address* addr) {
#ifdef DEBUGLOG2
    log->LOG(&memberNode->addr, "Table %d.%d.%d.%d:%d",  addr->addr[0],addr->addr[1],addr->addr[2],
            addr->addr[3], *(short*)&addr->addr[4]);
    vector<MemberListEntry>::iterator it = list->begin();
    log->LOG(&memberNode->addr, "|id\t|port\t|timestamp\t|heartbeat\t|");
    while (it != list->end()) {
        log->LOG(&memberNode->addr, "|%d\t|%d\t|%d\t\t|%d\t\t|", it->id, it->port, it->timestamp, it->heartbeat);
        it++;
    }
#endif
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

void MP1Node::sprintAddress(char const * format, char* buffer, Address *addr)
{
    sprintf(buffer, format,  addr->addr[0],addr->addr[1],addr->addr[2],
            addr->addr[3], *(short*)&addr->addr[4]) ;
}

void MP1Node::mergeTable(vector<MemberListEntry> *table) {

    vector<MemberListEntry>::iterator nit = table->begin();
    while (nit != table->end()) {
        Address addr;
        memcpy(addr.addr, &nit->id, sizeof(int));
        memcpy(addr.addr + 4, &nit->port, sizeof(short));

        if (0 == strcmp((char *) &memberNode->addr.addr, (char *) &addr.addr)) {
            nit++;
            continue;
        }

#ifdef DEBUGLOG2
        char buffer[1024];
        sprintAddress("Received table %d.%d.%d.%d:%d", buffer, &addr);
        log->LOG(&memberNode->addr, buffer);
        log->LOG(&memberNode->addr, "Received heartbeat id %d port %d", nit->id, nit->port);
#endif

        vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
        while (it != memberNode->memberList.end()) {
            if (it->id == nit->id) {
                break;
            }
            it++;
        }

        if (it == memberNode->memberList.end() && !isDeleted(nit->id)) {
#ifdef DEBUGLOG2
            log->LOG(&memberNode->addr, "New node received");
#endif
            memberNode->memberList.push_back(MemberListEntry(nit->id, nit->port, nit->heartbeat, par->getcurrtime()));
            log->logNodeAdd(&memberNode->addr, &addr);
        } else if (it->heartbeat < nit->heartbeat) {
#ifdef DEBUGLOG2
            log->LOG(&memberNode->addr, "Update timestamp");
#endif
            removeFromDeleted(it->id);
            *it = MemberListEntry(it->id, it->port, nit->heartbeat, par->getcurrtime());
        }
        nit++;
    }
}

