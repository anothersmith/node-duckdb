export declare class ConnectionWrapper {
    execute(command: string): ResultWrapper;
}
  
export declare class ResultWrapper {
    public fetchRow(): any[];
    public describe(): string[][];
}
