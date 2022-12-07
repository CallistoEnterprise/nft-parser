import os
import time
import dataset
import requests
import simplejson as json
import cachetools.func
from web3 import Web3,exceptions as web3_exceptions
from dotenv import load_dotenv
from datetime import datetime

PATH = os.path.dirname(os.path.realpath(__file__))
os.chdir(PATH)

ss = requests.Session()

load_dotenv('.env')

#proxy_abi = json.load(open('proxy.abi'))
#nft_abi = json.load(open('nft.abi'))

callisto_nft_abi = json.load(open('abis/callistoNFT.abi'))

mainnet_rpc= os.environ.get('rpc') 

w3 = Web3(Web3.HTTPProvider(mainnet_rpc,request_kwargs={'timeout': 300}))

class Web3Lib():

    def get_block_timestamp(self,number):
        return w3.eth.get_block(number).timestamp

    def get_latest_block_number(self):
        return w3.eth.get_block('latest').number


class Nft():

    clo_nft_version = 2 # 1 will be used as legacy
    next_mint_id =  -1

    def is_callisto_nft(self,contract_address):
        is_callisto_nft = False

        nft_contract = w3.eth.contract(
            address=contract_address, abi=callisto_nft_abi
        )

        calls = {
            'v2':self.nft_contract.functions.next_mint_id().call,
            'v1':self.nft_contract.functions.last_mind_id().call,
            'standard':self.nft_contract.functions.standard().call
        }

        # CallistoNFT
        
        #print(self.nft_contract.functions.getClassPropertiesForTokenID(token_id).call())
        #print(self.nft_contract.functions.getClassPropertyForTokenID(token_id,5).call())
        for call in calls:
            try:
                calls[call]()
            except web3_exceptions.ContractLogicError:
                return False

    def __init__(self,contract_address):
        
        self.contract_address = contract_address
        self.nft_contract = w3.eth.contract(
            address=contract_address, abi=callisto_nft_abi
        )

    def get_nft_owner(self,token_id):
        return self.nft_contract.functions.ownerOf(token_id).call()

    def get_transaction_input(self,hash):
        #self.nft_contract.decode_function_input()
        return self.nft_contract.decode_function_input(w3.eth.get_transaction(hash).input)

    def get_nft_bids(self,block_number=1):
        result = []
        nft_bid_events = self.nft_contract.events.bid.createFilter(fromBlock=block_number)#'latest') #fromBlock=8913084, toBlock=to
        bid_enteries = nft_bid_events.get_all_entries()
        for item in bid_enteries:
            transaction_hash = item['transactionHash'].hex()
            block_number = item['blockNumber']
            args = item['args']
            timestamp = self.get_block_timestamp(block_number)

            transaction_data = self.get_transaction_input(transaction_hash)

            entry = {
                'contract_address':self.contract_address,
                'tx_hash' : transaction_hash,
                'block_number' : block_number,
                'timestamp'    : timestamp,
                'user'         : args['_bidder'],
                'amount'       : float(w3.fromWei(args['_bid'],'ether')),
                'artwork_name': transaction_data[1]['_artwork_name'],
                'type'         : transaction_data[0].fn_name.replace("buy",""),
                'round'        : args['_round'],
                'start'        : args['_start'],
                'duration'     : args['_duration'],
                'ends'         : args['_start']+args['_duration']
            }
            result.append(entry)
            #print(transaction_hash,args,block_number,timestamp)
        return result

    def get_nft_events(self,block_number=1):
        result = []
        print(block_number)
        nft_transfer_events = self.nft_contract.events.Transfer.createFilter(fromBlock=block_number)#'latest') #fromBlock=8913084, toBlock=to
        staking_enteries = nft_transfer_events.get_all_entries()
        for item in staking_enteries:
            transaction_hash = item['transactionHash'].hex()
            block_number = item['blockNumber']
            args = item['args']
            timestamp = self.get_block_timestamp(block_number)

            entry = {
                'contract_address':self.contract_address,
                'token_id': args['tokenId'],
                'tx_hash' : transaction_hash,
                'block_number': block_number,
                'timestamp'   : timestamp,
                'to'          : args['to'],
                'from'        : args['from']
            }
            result.append(entry)
            #print(transaction_hash,args,block_number,timestamp)
        return result


    @cachetools.func.ttl_cache(ttl=15)
    def get_nft_info(self,token_id):
        category = ''
        serial_number = ''
        #self.w3.fromWei(self.w3.eth.get_balance(address), 'ether')
        #print(self.nft_contract.functions.getTokenProperties(token_id).call())

        print(self.nft_contract.functions.getClassPropertiesForTokenID(token_id).call())
        print(self.nft_contract.functions.getClassPropertyForTokenID(token_id,5).call())
        try:
            # something
            print()

        except web3_exceptions.ContractLogicError:
            return False

        if len(self.nft_contract.functions.getTokenProperties(token_id).call()[0]) == 2:
            metadata,uri = self.nft_contract.functions.getTokenProperties(token_id).call()[0]
        elif len(self.nft_contract.functions.getTokenProperties(token_id).call()[0]) == 3:
            metadata,uri,category = self.nft_contract.functions.getTokenProperties(token_id).call()[0]
        elif len(self.nft_contract.functions.getTokenProperties(token_id).call()[0]) == 4:
            metadata,uri,category,serial_number = self.nft_contract.functions.getTokenProperties(token_id).call()[0]

        owner = self.get_nft_owner(token_id)
        info = {
            'owner_of':owner,
            'token_id':token_id,
            'metadata':json.loads(metadata),
            'token_uri':uri,
            'parsed_at':datetime.now(),
            'category': category,
            'serial_number':serial_number
        }

        info.update(self.get_nft_contract_info())
        return info

    def get_nfts(self,new_only=False):

        tokens = []

        contract_info =  self.get_nft_contract_info()

        next_mint_id,contract_address =contract_info['next_mint_id'],contract_info['contract_address']


        db = NftDB('nft_data.db')

        if new_only:
            
            
            last_indexed_id = db.nft_owners.find(contract_address=contract_address,order_by=['-token_id'])

            last_indexed_id = list(last_indexed_id)

            if len(last_indexed_id) >0:
                last_indexed_id = last_indexed_id[0]['token_id']

                for token_id in range(last_indexed_id,next_mint_id):
                    tokens.append(self.get_nft_info(token_id))

        else:
            for token_id in range(next_mint_id):
                tokens.append(self.get_nft_info(token_id))
            
        db.db.close()
        return tokens


    @cachetools.func.ttl_cache(ttl=15)
    def get_nft_contract_info(self):
        #self.w3.fromWei(self.w3.eth.get_balance(address), 'ether')
        info = {
            'next_mint_id'      : self.nft_contract.functions.next_mint_id().call(),
            'name'              : self.nft_contract.functions.name().call(),
            'standard'          : self.nft_contract.functions.standard().call(),
            'symbol'            : self.nft_contract.functions.symbol().call(),
            'contract_address'  : self.contract_address
        }
        return info



class NftDB():
	
    def __init__(self,path):
        self.rows = []
        
        db = dataset.connect('sqlite:///' + path)
        
        if 'nft_contracts' not in db.tables:
            nft_contracts = db.create_table('nft_contracts',
                            primary_id='contract_address',
                            primary_type=db.types.string(42))
            
            nft_contracts.create_column('last_minted_id', db.types.bigint)
            nft_contracts.create_column('standard', db.types.string(200)) 
            nft_contracts.create_column('symbol', db.types.string(200))
            nft_contracts.create_column('name', db.types.string(200))
        else:
            self.nft_contracts = db.load_table('nft_contracts')

        if 'nft_owners' not in db.tables:
            nft_owners = db.create_table('nft_owners',primary_id=False)
            
            nft_owners.create_column('contract_address', db.types.string(42),unique=True)
            nft_owners.create_column('token_id', db.types.bigint) 
            nft_owners.create_column('symbol', db.types.string(200))
            nft_owners.create_column('name', db.types.string(200))
            
            nft_owners.create_column('owner_of', db.types.string(42))
            nft_owners.create_column('token_uri', db.types.text)
            nft_owners.create_column('category', db.types.string(200))
            nft_owners.create_column('metadata', db.types.text)
            nft_owners.create_column('synced_at', db.types.datetime)
            nft_owners.create_column('amount', db.types.integer)
            nft_owners.create_column('mint_transaction', db.types.string(66))
            nft_owners.create_column('mint_transaction_to', db.types.string(42))
            nft_owners.create_column('last_transaction', db.types.string(66))
            nft_owners.create_column('last_transaction_to', db.types.string(42))
            nft_owners.create_column('serial_number',db.types.bigint)
            
            self.nft_owners = nft_owners
            
        else:
            self.nft_owners = db.load_table('nft_owners')

        if 'nft_events' not in db.tables:
            nft_events = db.create_table('nft_events',primary_id=False)
            
            nft_events.create_column('contract_address', db.types.string(42))
            nft_events.create_column('token_id', db.types.bigint) 
            nft_events.create_column('tx_hash', db.types.string(66))
            nft_events.create_column('block_number', db.types.bigint)
            nft_events.create_column('timestamp', db.types.bigint)
            nft_events.create_column('to', db.types.string(42))
            nft_events.create_column('from', db.types.string(42))
            """nft_events.create_column('token_uri', db.types.text)
            nft_events.create_column('category', db.types.string(200))
            nft_events.create_column('metadata', db.types.text)
            nft_events.create_column('synced_at', db.types.datetime)
            nft_events.create_column('amount', db.types.integer)"""
            
            self.nft_events = nft_events
            
        else:
            self.nft_events = db.load_table('nft_events')

        if 'nft_bids' not in db.tables:
            nft_bids = db.create_table('nft_bids',primary_id=False)
            nft_bids.create_column('contract_address', db.types.string(42))
            nft_bids.create_column('tx_hash', db.types.string(66))
            nft_bids.create_column('user', db.types.string(42))
            nft_bids.create_column('type', db.types.string(200))
            nft_bids.create_column('artwork_name', db.types.text)
            nft_bids.create_column('amount', db.types.float) 

            self.nft_bids = nft_bids
        else:
            self.nft_bids = db.load_table('nft_bids')

        
        self.db = db

    @cachetools.func.ttl_cache(ttl=15)
    def get_nfts(self,contract_address,owner=-1):
        if owner == -1:
            return self.get_nft_owners(contract_address)
        else:
            return self.get_nft_owners(contract_address,owner=owner)

    def index_bids(self,contract_address):
        nft_query = Nft(contract_address)
        block_number = 1

        block_query = self.nft_bids.find(contract_address=contract_address,order_by=['-block_number'])
        if block_query:
            block_query = list(block_query)
            if block_query != []:
                block_number = block_query[0]['block_number']        

        for item in nft_query.get_nft_bids(block_number):
            self.nft_bids.upsert(item, ['contract_address','tx_hash'])
            print(contract_address,item)
            #self.nft_owners.update({'contract_address':contract_address,'token_id':item['token_id'],'owner_of':item['to']},['contract_address','token_id'])

    def index_events(self,contract_address):
        nft_query = Nft(contract_address)

        block_number = 1

        block_query = self.nft_events.find(contract_address=contract_address,order_by=['-block_number'])
        if block_query:
            block_query = list(block_query)
            if block_query != []:
                block_number = block_query[0]['block_number']

        for item in nft_query.get_nft_events(block_number):
            self.nft_events.upsert(item, ['contract_address','tx_hash'])
            self.nft_owners.update({'contract_address':contract_address,'token_id':item['token_id'],'owner_of':item['to']},['contract_address','token_id'])

    def get_mint_tx_info(self,contract_address,token_id):
        tx = self.nft_events.find(contract_address=contract_address,
            token_id=token_id,order_by=['block_number'])

        if tx:
            tx = list(tx)
            return tx[0]['tx_hash'],tx[0]['from'],tx[0]['to']

    def get_lastest_tx_info(self,contract_address,token_id):
        tx = self.nft_events.find(contract_address=contract_address,
            token_id=token_id,order_by=['-block_number'])

        if tx:
            tx = list(tx)
            return tx[0]['tx_hash'],tx[0]['from'],tx[0]['to']

    def index_nfts(self,contract_address):
        nft_query = Nft(contract_address)
        self.index_events(contract_address)
        self.index_bids(contract_address)
        contract_info = nft_query.get_nft_contract_info()

        serial ={}

        for nft in nft_query.get_nfts(new_only=True):

            #mint_tx_info = self.get_mint_tx_info(contract_address,nft['token_id'])
            last_tx_info = self.get_lastest_tx_info(contract_address,nft['token_id'])
            
            #if not mint_tx_info == last_tx_info:
            #    print(mint_tx_info,last_tx_info)
            #print(mint_tx_info)
            #print(last_tx_info)
            serial_hash= hashlib.sha256(json.dumps(nft['metadata']).encode()).hexdigest()
            if serial_hash not in serial:
                serial[serial_hash] = 1
            else:
                serial[serial_hash] += 1

            q = {
                'contract_address':nft['contract_address'],
                'symbol':nft['symbol'],
                'synced_at':nft['parsed_at'],
                'parsed_at':nft['parsed_at'],
                'token_id':nft['token_id'],
                'owner_of':last_tx_info[2],
                'token_uri':nft['token_uri'],
                'name':nft['name'],
                'metadata':json.dumps(nft['metadata']),
                'category':nft['category'],
                'last_minted_id':nft['last_minted_id'],
                'standard':nft['standard'],
                'amount':1,
                'transaction_hash':last_tx_info[0],
                'serial_number':serial[serial_hash]
            }

            if q['category'] == '':
                
                if q['token_uri'] in ['https://gateway.pinata.cloud/ipfs/QmX3wjkW6aKfaGQkfHvzm4QX18qoDfBWuHWQZTZLW3VV19','https://gateway.pinata.cloud/ipfs/QmTSCGhRYg9a13EMuqL8psG6SQNsFHUXZFHSE3tAgpcYeB/TV_020p.gif','https://gateway.pinata.cloud/ipfs/QmNu9kpApsibUw2yzw8Nsqmf2qesXdVR5dxmdgk48uK9EU','https://gateway.pinata.cloud/ipfs/QmaHiTGYegGEkKXVESBB2ozStZusPr2ZFJ44pTxGNVdLey','https://gateway.pinata.cloud/ipfs/QmZezDn6uXQPX137XNaRTigo5myLYGGzT1hqBZmwoJEpDB']:
                    q['category'] = 'Bronze'
                elif q['token_uri'] in ["https://gateway.pinata.cloud/ipfs/Qmb1P2LowiqTwzD7K4SF5JxzzTUKoZH9RGcgGCC8GfKauZ","https://gateway.pinata.cloud/ipfs/QmTSCGhRYg9a13EMuqL8psG6SQNsFHUXZFHSE3tAgpcYeB/TV_030p.gif","https://gateway.pinata.cloud/ipfs/QmdfQxFQz97wcrtDkCviguMscvRzsMWZCv6oTSoLHxz9fM","https://gateway.pinata.cloud/ipfs/QmcJUJJQQsuxYB6nx735MbfHwSXLCX1yWJ4NNvf7AuVrV5","https://gateway.pinata.cloud/ipfs/QmcWgjXf4qtznkL1fgaMn6Sd8MDjHqsNwycpLwMgoWtqdJ"]:
                    q['category'] = 'Silver'
                elif q['token_uri'] in ["https://gateway.pinata.cloud/ipfs/Qmd25o9JVs7YiMAoTyfMQ4mFBHxUAFmXhV2P27gYAhT9Sz","https://gateway.pinata.cloud/ipfs/QmTSCGhRYg9a13EMuqL8psG6SQNsFHUXZFHSE3tAgpcYeB/TV_040p.gif","https://gateway.pinata.cloud/ipfs/QmfMfuU785GFTW5VPLEMLhu5KaRUgxBA21X6QeoRXBmrMn","https://gateway.pinata.cloud/ipfs/QmZNs75TvqPddEbf7L7xif6Ae4NZxiKZQCNYcEk124GNC6","https://gateway.pinata.cloud/ipfs/Qmbjo6fLhZQP58Rva9MeiM4w28hJcoYFjL1zjw9kF25qb2"]:
                    q['category'] = 'Gold'

            self.nft_owners.upsert(q, ['contract_address','token_id'])
        #print("done indexing")
        self.nft_contracts.upsert(contract_info, ['contract_address'])


    def get_nft_bid_status(self,transaction):

        status = 'N/A'

        bid_query = self.nft_bids.find_one(tx_hash = transaction)

        if bid_query:
            contract_address = bid_query['contract_address']
            artwork_type = bid_query['type']
            artwork_name = bid_query['artwork_name']
            round = bid_query['round']
            ends = bid_query['ends']

            #print(contract_address,artwork_name,artwork_type)
            bids_query = self.nft_bids.find(contract_address=contract_address,round=round, type=artwork_type , artwork_name=artwork_name,order_by=['-amount'])

            if bids_query:

                response = list(bids_query)[0]
                #print(response['tx_hash'])
                if response['tx_hash'] == transaction and datetime.utcnow().timestamp() < ends:
                    status = 'Winning'
                elif response['tx_hash'] == transaction and datetime.utcnow().timestamp() >= ends:
                    status = 'Won'
                elif response['tx_hash'] != transaction and datetime.utcnow().timestamp() < ends:
                    status = 'Loosing'
                elif response['tx_hash'] != transaction and datetime.utcnow().timestamp() >= ends:
                    status = 'Ended'

            #print(datetime.utcnow().timestamp())


        return status,'OK'
        

    @cachetools.func.ttl_cache(ttl=15)
    def get_nft_bidders(self,contract_address,user=-1):

        result = []
        
        if user==-1:
            bidders_db_query = self.nft_bids.find(contract_address=contract_address)
        else:
            bidders_db_query = self.nft_bids.find(contract_address=contract_address,user = Web3.toChecksumAddress(user))

        if bidders_db_query:
            for item in bidders_db_query:
                entry = dict(item)
                entry['token_address']= entry['contract_address']
                del(entry['contract_address'])
                entry['timestamp'] = datetime.utcfromtimestamp(item['timestamp'])
                entry['start'] = datetime.utcfromtimestamp(item['start'])
                entry['ends'] = datetime.utcfromtimestamp(item['ends'])
                result.append(entry)
        return result,'OK'

    @cachetools.func.ttl_cache(ttl=15)
    def get_nft_owners(self,contract_address,token_id=-1,owner=-1):

        result = []

        nft_query = Nft(contract_address)
        contract_db_query= self.nft_contracts.find_one(contract_address=contract_address)
        # todo check if contract is indexed, and valid NFT (might be proxy even)
        if not contract_db_query:
            # check owners
            owners = self.nft_owners.find(token_address=contract_address,token_id=token_id)
            if not owners:
                print(owners)
            else:
                now = datetime.now()
                contract_info = nft_query.get_nft_contract_info()

                self.nft_contracts.upsert(contract_info, ['contract_address'])

                #for nft in nft_query.get_nfts():
                #    print(nft)
                #index owners
                #return response
                pass
        
        else:
            # {'owner_of': '0xA51207a20B7d971A4aDCfCe2e36c8Ae4f78324dB', 'token_id': 348, 'metadata': {'author': 'Krištof Kintera', 'work_title': 'Drawing 2 (Kresba 2)',
            # 'work_description': "This is the second Drawing from unique Krištof Kintera's Drawing collection that will include a total of 20 drawings specially created as NFT.", 'license': '',
            # 'nft_type': 'Art', 'collection': "Krištof Kintera's Drawing"}, 'token_uri': 'https://gateway.pinata.cloud/ipfs/QmZezDn6uXQPX137XNaRTigo5myLYGGzT1hqBZmwoJEpDB',
            # 'parsed_at': datetime.datetime(2021, 12, 21, 14, 19, 36, 134726), 'category': 'Bronze', 'last_minted_id': 349, 'name': 'ArteFin', 'standard': 'NFT X',
            # 'symbol': 'ART', 'contract_address': '0x7E7C9f515d06bAD40f8e1e7477421CF64Ca59E5D'}

            # get contract info #{'last_minted_id': 349, 'name': 'ArteFin', 'standard': 'NFT X', 'symbol': 'ART', 'contract_address': '0x7E7C9f515d06bAD40f8e1e7477421CF64Ca59E5D'}
            # contract db query #OrderedDict([('contract_address', '0x7E7C9f515d06bAD40f8e1e7477421CF64Ca59E5D'), ('last_minted_id', 348), ('standard', 'NFT X'), ('symbol', 'ART'), ('name', 'ArteFin')])
            contract_info = nft_query.get_nft_contract_info()
            #print(contract_db_query)
            #print(contract_info)

            
            if contract_info['last_minted_id'] == contract_db_query['last_minted_id']:
                pass
            else:
                
                # reindex the NFTs
                # return the NFTs
                pass
            
            #time.sleep(99)
            # make sure has the same number of minted NFTs
            
            if owner==-1 and token_id == -1:
                owners_db_query = self.nft_owners.find(contract_address=contract_address)
            elif token_id != -1:
                owners_db_query = self.nft_owners.find(contract_address=contract_address,token_id = token_id)
            elif owner != -1:
                owners_db_query = self.nft_owners.find(contract_address=contract_address,owner_of = Web3.toChecksumAddress(owner))

            if owners_db_query:
                for item in owners_db_query:
                    entry = dict(item)
                    entry['token_address']= entry['contract_address']
                    del(entry['last_minted_id'])
                    del(entry['contract_address'])
                    entry['metadata'] = json.loads(entry['metadata'])
                    result.append(entry)
            pass
        #contract_info = nft_query.get_nft_contract_info()
        #self.nft_contracts.upsert(contract_info, ['contract_address'])

        """for nft in nft_query.get_nfts():
            del(nft['last_minted_id'])
            nft['token_address']= nft['contract_address']
            nft['amount'] = 1
            del(nft['contract_address'])
            
            if token_id == -1 and owner == -1:
                result.append(nft)
            elif token_id != -1 and nft['token_id'] == token_id:
                result.append(nft)
            elif owner != -1 and nft['owner_of'].lower() == owner.lower():
                result.append(nft)"""

        return result,'OK'


 

if __name__ == '__main__':



    x = Nft('0x096194aa4dFd64b506630149B921015170753a11')
    #x = Nft('0x69c878B44fb427cd084125Ce3b76a8Ee4685E78c')
    x.get_nfts()

    # Nft('0x')

    """
    #nft = Nft('0x0a05E546115A66262a980A61D264DE5244e7a3D0')
    nft = Nft('0xE33A9B4EebdD52c68dB880Da3DfB91070D7832C7') #test
    #print(nft.get_nft_contract_info())
    #print(nft.get_nft_info(1))
    #print(nft.get_nfts())

    #print(nft.get_nft_events())

    #print(nft.get_nft_bids())
    #time.sleep(999)



    db = NftDB('nft_data.db')"""

    """for item in db.get_nfts('0x0a05E546115A66262a980A61D264DE5244e7a3D0','0x1e91DA1677986b36372A2Dcf6CA0d3DaB8E1E5fC')[0]:
        print(item["token_id"])
    time.sleep(999)"""
    
    #print(dir(db.db))

    #print(db.get_nft_bid_status('0xf5b91dedb32a36bac1533463559b6326e762fddb2f72894ebfe4475b08a05533'))
    #time.sleep(999)


    """
    while True:
        t0 = time.time()
        
        for item in db.nft_contracts.all():
            #try:
            contract_address = item["contract_address"]

            #print(db.get_nft_bidders(contract_address,'0x03156586FD43AbceFE41563564Ebb2240042415e'))

            db.index_nfts(contract_address)
            #except Exception as e:
            #    print(str(e))
            #   time.sleep(15)
        
        print("time",time.time()- t0)
        time.sleep(15)

    #db.index_events('0x7E7C9f515d06bAD40f8e1e7477421CF64Ca59E5D')

    
    for contract in ['0x0a05E546115A66262a980A61D264DE5244e7a3D0','0x7E7C9f515d06bAD40f8e1e7477421CF64Ca59E5D']:
        print(contract)
        print(len(db.get_nft_owners(contract)[0]))
        #print(len(db.get_nfts(contract)[0]))
        #print(len(db.get_nfts(contract,'0xc9743422A6e936e043B20eF06204da2a00E125cF')[0]))
        
    """