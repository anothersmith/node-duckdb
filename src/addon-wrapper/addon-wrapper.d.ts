export class ConnectionWrapper {
    public execute(command: string): ResultWrapper;
}

export class ResultWrapper {
    public fetchRow(): any[];
    public describe(): string[][];
}
