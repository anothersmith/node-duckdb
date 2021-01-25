/* eslint-disable no-console */
import { read, stat, open } from "fs";

import { IFileSystem } from "@addon-types";
import glob from "glob";

export const fileSystem: IFileSystem = {
  readWithLocation: (
    fd: number,
    buffer: Buffer,
    length: number,
    position: number,
    callback: (err: Error | null, buffer: Buffer) => void,
  ) => {
    console.log("sdadsadsa");
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
    console.log("sdadsadsa");
    read(fd, buffer, 0, length, null, (error, bytesRead, filledBuffer) => {
      callback(error, filledBuffer, bytesRead);
    });
  },
  glob: (path: string, callback) => {
    console.log("sdadsadsa");
    glob(path, (error, matches) => {
      callback(error, matches);
    });
  },
  getFileSize: (path: string, callback: (error: Error | null, size: number) => void) => {
    console.log("sdadsadsa");
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
    console.log("sdadsadsa");
    open(path, flags, (error, fd) => {
      callback(error, fd);
    });
  },
};
