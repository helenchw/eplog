#include <assert.h>
#include <string.h>
#include <algorithm>
#include <set>
#include "common/define.hh"
#include "common/debug.hh"
#include "coding.hh"
#include "evenoddcoding.hh"

using namespace std;

EvenOddCoding::EvenOddCoding() {

}

EvenOddCoding::~EvenOddCoding() {

}

vector<Chunk> EvenOddCoding::encode(char* buf, CodeSetting codeSetting) {
	const int n = codeSetting.n;
	const int k = codeSetting.k;
	const int chunkSize = getChunkSize(k, codeSetting.isLog);
	const int symbolSize = chunkSize / k;
	assert (k == n-2);

	vector<Chunk> chunkList (n);
	for (int i = 0; i < n; i++) {
	    chunkList[i].chunkId = i;
	    chunkList[i].length = chunkSize;
	    chunkList[i].buf = (char*) buf_calloc (1, chunkSize);
	}

	int d_group;
	for(int i = 0; i < k; i++) {
		d_group = i;

		// Copy Data
		memcpy (chunkList[i].buf, buf + i * chunkSize, chunkSize);

		// Compute Row Parity Block
		bitwiseXor(chunkList[k].buf, chunkList[k].buf, chunkList[i].buf, chunkSize);

		// Compute Diagonal Parity Block
		for(int j = 0; j < k-1; j++) {
			if(d_group == k-1) {
                for (int l = 0; l < k - 1; l++) {
                    bitwiseXor(chunkList[k + 1].buf + l * symbolSize,
                            chunkList[k + 1].buf + l * symbolSize,
                            chunkList[i].buf + j * symbolSize, symbolSize);
                }
			} else {
                bitwiseXor(chunkList[k + 1].buf + d_group * symbolSize,
                        chunkList[k + 1].buf + d_group * symbolSize,
                        chunkList[i].buf + j * symbolSize, symbolSize);
			}
			d_group = (d_group + 1) % k;
		}
	}

	return chunkList;
}

void EvenOddCoding::decode(vector<ChunkSymbols> reqSymbols, vector<Chunk> chunks,
        char* buf, CodeSetting codeSetting, off_len_t offLen) {

    const int n = codeSetting.n;
    const int k = codeSetting.k;
    const int chunkSize = getChunkSize(k, codeSetting.isLog);

    if (chunks.back().chunkId == n-1 || chunks.back().chunkId == n-2) { // if parity is involved

        // resize chunks since repairDataBlock require sparse chunk vector
        chunks.resize(n);
        for (unsigned i = chunks.size(); i-- > 0; ) {
            if (chunks[i].chunkId != -1 && chunks[i].chunkId != (int)i) {
                swap (chunks[i], chunks[chunks[i].chunkId]);
            }
        }

        char** rstBlock = this->repairDataBlocks(chunks, reqSymbols, codeSetting); // call the suitable repairDataBlocks

        const int startChunk = offLen.first / chunkSize;
        const int endChunk = (offLen.first + offLen.second - 1) / chunkSize;
        int curOff = offLen.first;
        int curLen = offLen.second;
        int bufOff = 0;

        for (int i = startChunk; i <= endChunk; i++) {
            const int off = curOff - chunkSize * i;
            const int len = min (curLen, chunkSize * (i+1) - curOff); // curLen vs len to chunk end
            memcpy(buf + bufOff, rstBlock[i] + off, len);
            curOff = curOff + len;
            curLen -= len;
            bufOff += len;
        }

        for(int i = 0; i < n; i++) {
            free(rstBlock[i]);
        }
        free(rstBlock);
    } else {  // no failure, use RAID-0 logic
        StripingCoding::decode(reqSymbols, chunks, buf, codeSetting, offLen);
    }
}

vector<ChunkSymbols> EvenOddCoding::getReqSymbols(vector<bool> chunkStatus,
        CodeSetting codeSetting, off_len_t offLen) {


    const int n = codeSetting.n;
    const int k = codeSetting.k;
    const int chunkSize = getChunkSize(k, codeSetting.isLog);
    const int fail = (int) count(chunkStatus.begin(), chunkStatus.end(), false);
    if (fail > n - k) {
        debug_error("Too many failures: %d\n", fail);
        exit(-1);
    }

    // use RAID-0 logic if no data chunk failures
    assert ((int)chunkStatus.size() == n);
    bool useRaid0Decode = true;
    for (int i = 0; i < k; i++) {
        debug ("i = %d, status = %s\n", i, chunkStatus[i]?"true":"false");
        if (chunkStatus[i] == false) {
            useRaid0Decode = false;
            break;
        }
    }
    if (useRaid0Decode) {
        debug ("%s\n", "Using raid0 decode");
        for (int j = 0; j < 2; j++) {
            chunkStatus.pop_back(); // ignore 2 parities
        }
        return StripingCoding::getReqSymbols(chunkStatus, codeSetting, offLen);
    }

    vector<ChunkSymbols> reqSymbols;
    reqSymbols.reserve(k);

    int selected = 0;
    for(int i = 0; i < n; ++i) {
        if(chunkStatus[i] == true) {
            off_len_t symbol = make_pair(0, chunkSize);
            vector<off_len_t> symbolList = { symbol };
            ChunkSymbols chunkSymbols (i, symbolList);
            reqSymbols.push_back(chunkSymbols);
            if (++selected == k)
                break;
        }
    }

    return reqSymbols;
}

char** EvenOddCoding::repairDataBlocks(vector<Chunk> chunks,
        vector<ChunkSymbols> reqSymbols, CodeSetting codeSetting,
        bool recovery) {
	const int n = codeSetting.n;
	const int k = codeSetting.k;
	const int chunkSize = getChunkSize(k, codeSetting.isLog);
	const int symbolSize = chunkSize / k;

	int row_group_erasure[k - 1];
	int diagonal_group_erasure[k];
	int datadisk_block_erasure[k];
	bool disk_block_status[n][k - 1];
	bool need_diagonal_adjust[k][k - 1];
	int numFailData = k;
	fill_n (row_group_erasure, k - 1, k + 1);
	fill_n (diagonal_group_erasure, k - 1, k); // miss 1?
	fill_n (datadisk_block_erasure, k, k - 1);
	char* diagonal_adjuster = (char*) malloc (symbolSize);
	char* repair_symbol = (char*) malloc (symbolSize);

	bool use_diagonal = false;
	bool use_row = false;

	char** rstBlock;
    rstBlock = (char**) malloc(sizeof(char*) * n);
	for(int i = 0; i < n; i++) {
		rstBlock[i] = (char*) buf_malloc(chunkSize);
		fill_n (disk_block_status[i], k - 1, false);
		if(i < k)
			fill_n(need_diagonal_adjust[i], k - 1, false);
	}
	diagonal_group_erasure[k - 1] = k - 1;

	for(ChunkSymbols chunkSymbols: reqSymbols) {
	    const int id = chunkSymbols.chunkId;
		if(id == k + 1)
			use_diagonal = true;
		else if (id == k)
			use_row = true;
	}

	for(ChunkSymbols chunkSymbols: reqSymbols) {
	    const int id = chunkSymbols.chunkId;
		const vector<off_len_t> symbolList = chunkSymbols.offLens;
		if(id < k) {
			datadisk_block_erasure[id] = 0;
			--numFailData;
		}
		int offset = 0;
		for(auto symbol : symbolList) {
			memcpy(rstBlock[id] + symbol.first, chunks[id].buf + offset, symbol.second);
			offset += symbol.second;
			int firstSymbolId = symbol.first / symbolSize;
			for(int i = 0; i < (symbol.second / symbolSize); ++i) {
				disk_block_status[id][firstSymbolId + i] = true;
				if(use_row && (id != k + 1)) {
					int row_group = firstSymbolId + i;
					--row_group_erasure[row_group];
				}
				if(use_diagonal && (id != k)) {
					int diagonal_group = (firstSymbolId + i + id) % k;
					if(id == k + 1) {
						diagonal_group = firstSymbolId + i;
					}
					--diagonal_group_erasure[diagonal_group];
					if(diagonal_group == k - 1) {
						bitwiseXor(diagonal_adjuster, rstBlock[id] + (firstSymbolId + i) * symbolSize, diagonal_adjuster, symbolSize);
					}
				}
			}
		}
	}


	bool diagonal_adjuster_failed = !(diagonal_group_erasure[k - 1] == 0);
	bool diagonal_clean[k];
	std::fill_n(diagonal_clean, k , true);
	vector<pair<int,int> > needDiagonalFix;    // <blockId, symbolId>

	// Fix Failed Data Disk
	int id;
	int symbol;
	int target_id;
	int target_symbol;
	while(1) {
		if(numFailData == 0)
			break;

		target_id = (int)-1;
		target_symbol = (int)-1;
		memset(repair_symbol, 0, symbolSize);
		bool cur_symbol_need_adjust = false;
		for(int i = 0; i < k - 1; ++i) {
			// Fix by Row
			if(use_row && (row_group_erasure[i] == 1)) {
				cur_symbol_need_adjust = false;
				symbol = i;
				target_symbol = i;
				for(int j = 0; j < k + 1; ++j) {
					id = j;
					if((id < k) && (disk_block_status[id][symbol] == false))
						target_id = id;
					else {
						bitwiseXor(repair_symbol, repair_symbol, rstBlock[id] + symbol * symbolSize, symbolSize);
						if(need_diagonal_adjust[id][symbol]){
							cur_symbol_need_adjust = !cur_symbol_need_adjust;
						}
					}
				}

				if(use_diagonal) {
					int diagonal_group = (target_symbol + target_id) % k;
					// Fix Diagonal Parity Adjuster
					if(diagonal_group == k - 1) {
						bitwiseXor(diagonal_adjuster, repair_symbol, diagonal_adjuster, symbolSize);
					}
					--diagonal_group_erasure[diagonal_group];
					diagonal_clean[diagonal_group] = diagonal_clean[diagonal_group] != cur_symbol_need_adjust;
				}

				--row_group_erasure[symbol];
				break;
			}

			// Fix by Diagonal
			if(use_diagonal && (diagonal_group_erasure[i] == 1)) {
				cur_symbol_need_adjust = true;
				id = i;
				symbol = 0;
				for(int j = 0; j < k - 1; ++j){
					if(disk_block_status[id][symbol] == false) {
						target_id = id;
						target_symbol = symbol;
					} else {
						bitwiseXor(repair_symbol, repair_symbol, rstBlock[id] + symbol * symbolSize, symbolSize);
						if(need_diagonal_adjust[id][symbol]){
							cur_symbol_need_adjust = !cur_symbol_need_adjust;
						}
					}
					id = (id - 1 + k) % k;
					++symbol;
				}
				bitwiseXor(repair_symbol, repair_symbol, rstBlock[k + 1] + i * symbolSize, symbolSize);

				diagonal_clean[i] = diagonal_clean[i] != cur_symbol_need_adjust;

				diagonal_group_erasure[i] = 0;
				if(use_row)
					--row_group_erasure[target_symbol];
				break;
			}
		}

		// Fix Diagonal Adjuster
		if(use_diagonal && (target_id == (int)-1) && (diagonal_group_erasure[k - 1] == 1)) {
			cur_symbol_need_adjust = true;
			id = k - 1;
			symbol = 0;
			for(int i = 0; i < k - 1; ++i) {
				if(disk_block_status[id][symbol]){
					bitwiseXor(repair_symbol, repair_symbol, rstBlock[id] + symbol * symbolSize, symbolSize);
					if(need_diagonal_adjust[id][symbol]){
						cur_symbol_need_adjust = !cur_symbol_need_adjust;
					}
				} else {
					target_id = id;
					target_symbol = symbol;
				}
				--id;
				++symbol;
			}
		}

		if(target_id == (int)-1){
			debug_error("%s\n","Cannot fix any more");
			break;
		}

		if(cur_symbol_need_adjust) {
			need_diagonal_adjust[target_id][target_symbol] = true;
			needDiagonalFix.push_back(make_pair(target_id, target_symbol));
		}

		disk_block_status[target_id][target_symbol] = true;

		memcpy(rstBlock[target_id] + target_symbol * symbolSize, repair_symbol, symbolSize);

		--datadisk_block_erasure[target_id];
		if(datadisk_block_erasure[target_id] == 0){
			--numFailData;
		}
	}

	if(use_diagonal) {
		// Find a Clean Diagonal Parity
		if(diagonal_adjuster_failed){
			for(int i = 0; i < k - 1; ++i)
				if(diagonal_clean[i] && (diagonal_group_erasure[i] == 0)){
					memcpy(diagonal_adjuster, rstBlock[k + 1] + i * symbolSize, symbolSize);
					id = i;
					symbol = 0;
					for(int j = 0; j < k - 1; ++j){
						bitwiseXor(diagonal_adjuster, diagonal_adjuster, rstBlock[id] + symbol * symbolSize, symbolSize);
						id = (id - 1 + k) % k;
						symbol++;
					}
					break;
				}
		}

		// Apply Diagonal Parity Adjuster
		for(auto block_symbol : needDiagonalFix){
			char* bufPtr = rstBlock[block_symbol.first] + block_symbol.second * symbolSize;
			bitwiseXor(bufPtr, bufPtr, diagonal_adjuster, symbolSize);
		}
	}

	free(repair_symbol);
	free(diagonal_adjuster);
	return rstBlock;
}
