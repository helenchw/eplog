#ifndef ENUM_HH_
#define ENUM_HH_

/** Coding schemes supported */
enum CodingScheme {
    RAID0_CODING,
    RAID1_CODING,
    RAID5_CODING,
    RDP_CODING,
    EVENODD_CODING,
    CAUCHY_CODING,
    DEFAULT_CODING
};

/** Whether to enable disk logging approach (Deprecated) */
enum DiskLogMode {
    DISABLE,
    DELTA_LOG
};

/** Type of data/disk to write */
enum DataType{
    DATA,
    LOG
};

#endif /* ENUM_HH_ */
