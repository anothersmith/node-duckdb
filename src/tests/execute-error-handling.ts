import { ConnectionWrapper } from "../index";

describe("Execute method error handling", () => {
    it("validates parameters", async () => {
        const cw = new ConnectionWrapper();
        await expect((<any>cw).execute()).rejects.toEqual(TypeError("String expected"));
      });
    
})
