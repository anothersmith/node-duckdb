/* eslint-disable no-console */
/* eslint-disable jest/expect-expect */
import { a } from "./addon-wrapper";

jest.setTimeout(99999999999)
it("validates parameters", async () => {
    setInterval(() => {
        console.log(new Date());
    }, 0)
    const b = a();
    const c = a();
    console.log(await Promise.all([b, c]));
  });
