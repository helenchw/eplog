#include <assert.h>
#include <string.h>
#include <algorithm>
#include <set>
#include "common/define.hh"
#include "common/debug.hh"
#include "coding.hh"
#include "rdpcoding.hh"

using namespace std;

RdpCoding::RdpCoding() {

}

RdpCoding::~RdpCoding() {

}

vector<Chunk> RdpCoding::encode(char* buf, CodeSetting codeSetting) {
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
		for(int j = 0; j < k; j++) {
			if(d_group == k) {
				// Missing Diagonal
			} else {
                bitwiseXor(chunkList[k + 1].buf + d_group * symbolSize,
                        chunkList[k + 1].buf + d_group * symbolSize,
                        chunkList[i].buf + j * symbolSize, symbolSize);
			}
			d_group = (d_group + 1) % (k + 1);
		}
	}

	// Adjust to Diagonal Parity
    bitwiseXor(chunkList[k + 1].buf, chunkList[k].buf + symbolSize,
            chunkList[k + 1].buf, (k - 1) * symbolSize);

	return chunkList;
}

char** RdpCoding::repairDataBlocks(vector<Chunk> chunks,
        vector<ChunkSymbols> reqSymbols, CodeSetting codeSetting,
        bool recovery) {
    const int n = codeSetting.n;
    const int k = codeSetting.k;
    const int chunkSize = getChunkSize(k, codeSetting.isLog);
    const int symbolSize = chunkSize / k;

	int row_group_erasure[k];
	int diagonal_group_erasure[k + 1];
	bool disk_block_status[n][k];
	fill_n (row_group_erasure, k, k + 1);
	fill_n (diagonal_group_erasure, k + 1, k + 1);

	int numFail = n;
	int numFailData = k;
	int disk_block_erasure[n];
	fill_n (disk_block_erasure, n, k);

	char** rstBlock;
	rstBlock = (char**)buf_malloc(sizeof(char*) * n);

	for(int i = 0; i < n; ++i) {
		rstBlock[i] = (char*)buf_malloc(chunkSize);
		fill_n(disk_block_status[i], k, false);
	}

    for(ChunkSymbols chunkSymbols: reqSymbols) {
        const int id = chunkSymbols.chunkId;
        const vector<off_len_t> symbolList = chunkSymbols.offLens;
		disk_block_erasure[id] = 0;
		--numFail;
		if(id < k)
			--numFailData;
		int offset = 0;
		for(auto symbol : symbolList) {
		    debug ("id = %d, off = %d\n", id, symbol.first);
			memcpy(rstBlock[id] + symbol.first, chunks[id].buf + offset, symbol.second);
			offset += symbol.second;
			int firstSymbolId = symbol.first / symbolSize;
			for(int i = 0; i < (symbol.second / symbolSize); ++i) {
				int symbol_id = firstSymbolId + i;
				disk_block_status[id][symbol_id] = true;
				if(id != k + 1) {
					--row_group_erasure[symbol_id];
				}

				int d_group = (symbol_id + id) % (k + 1);
				--diagonal_group_erasure[d_group];
			}
		}
	}

	if(!recovery && (numFailData == 0))
		return rstBlock;

	int id;
	int symbol;
	int target_id;
	int target_symbol;
	char* repair_symbol = (char*)buf_malloc(symbolSize);
	while(1) {
		if(numFail == 0)
			break;

		target_id = (int) - 1;
		target_symbol = (int) - 1;
		memset(repair_symbol, 0, symbolSize);

		for(int i = 0; i < k; ++i) {
			// Fix by Row
			if(row_group_erasure[i] == 1) {
				symbol = i;
				for(int j = 0; j < k + 1; ++j) {
					id = j;
					if(disk_block_status[id][symbol] == false) {
						target_id = id;
						target_symbol = symbol;
					} else {
						bitwiseXor(repair_symbol, repair_symbol, rstBlock[id] + symbol * symbolSize, symbolSize);
					}
				}

				int d_group = (target_symbol + target_id) % (k + 1);
				--diagonal_group_erasure[d_group];

				row_group_erasure[symbol] = 0;
				break;
			}

			// Fix by Diagonal
			if(diagonal_group_erasure[i] == 1) {
				id = i;
				symbol = 0;
				for(int j = 0; j < k + 1; ++j) {
					if(disk_block_status[id][symbol] == false) {
						target_id = id;
						target_symbol = symbol;
					} else {
						bitwiseXor(repair_symbol, repair_symbol, rstBlock[id] + symbol * symbolSize, symbolSize);
					}
					if (id == 0) id = k + 1;
					else {
						--id;
						++symbol;
					}
				}
				diagonal_group_erasure[i] = 0;
				if(target_id != k + 1)
					--row_group_erasure[target_symbol];

				break;
			}
		}

		if(target_id == -1) {
			break;
		}

		disk_block_status[target_id][target_symbol] = true;
		memcpy(rstBlock[target_id] + target_symbol * symbolSize, repair_symbol, symbolSize);
		--disk_block_erasure[target_id];
		if(disk_block_erasure[target_id] == 0)
			--numFail;
	}
	free(repair_symbol);
	return rstBlock;
}
