import { mount } from "svelte";
import DetailPage from "./DetailPage.svelte";

const app = mount(DetailPage, {
  target: document.getElementById("app-root")!,
});

export default app;
