import { read, stat, open, ftruncate } from "fs";

import { IFileSystem } from "@addon-types";
// eslint-disable-next-line node/no-unpublished-import
import glob from "fast-glob";

export const fileSystem: IFileSystem = {
  readWithLocation: (
    fd: number,
    buffer: Buffer,
    length: number,
    position: number,
    callback: (err: Error | null, buffer: Buffer) => void,
  ) => {
    read(fd, buffer, 0, length, position, (error, _bytesRead, filledBuffer) => {
      callback(error, filledBuffer);
    });
  },
  read: (
    fd: number,
    buffer: Buffer,
    length: number,
    callback: (error: Error | null, buffer: Buffer, bytesRead: number) => void,
  ) => {
    read(fd, buffer, 0, length, null, (error, bytesRead, filledBuffer) => {
      callback(error, filledBuffer, bytesRead);
    });
  },
  glob: async (path: string, callback) => {
    try {
      const matches = await glob(path);
      callback(null, matches);
    } catch (e) {
      callback(e, []);
    }
  },
  getFileSize: (path: string, callback: (error: Error | null, size: number) => void) => {
    stat(path, (error, stats) => {
      callback(error, stats.size);
    });
  },
  // TODO: _fileLockType
  openFile: (
    path: string,
    flags: number,
    _fileLockType: number,
    callback: (error: Error | null, fd: number) => void,
  ) => {
    open(path, flags, (error, fd) => {
      callback(error, fd);
    });
  },
  truncate: (fd: number, len: number, callback: (error: Error | null) => void) => {
    ftruncate(fd, len, err => {
      callback(err);
    });
  },
};
