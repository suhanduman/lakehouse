import { Route, Routes } from "react-router-dom";
import Nav from "./components/Nav";
import AddSourceWizard from "./pages/AddSourceWizard";
import Schemas from "./pages/Schemas";
import SourceDetail, { type Role } from "./pages/SourceDetail";
import SourcesList from "./pages/SourcesList";
import StatusDashboard from "./pages/StatusDashboard";
import TablesBuckets from "./pages/TablesBuckets";

// TODO(post-Task 13): resolve from the OIDC session once the frontend wires
// up auth against the backend's app.services.authz roles (ADMIN/ANALYST/
// STUDENT). Hardcoded for now so SourceDetail's delete-modal admin gating
// has a real role to consume today.
const CURRENT_USER_ROLE: Role = "ADMIN";

export default function App() {
  return (
    <div>
      <Nav />
      <main>
        <Routes>
          <Route path="/" element={<SourcesList />} />
          <Route path="/sources/add" element={<AddSourceWizard />} />
          <Route
            path="/sources/:name"
            element={<SourceDetail role={CURRENT_USER_ROLE} />}
          />
          <Route path="/tables" element={<TablesBuckets />} />
          <Route path="/schemas" element={<Schemas />} />
          <Route path="/status" element={<StatusDashboard />} />
        </Routes>
      </main>
    </div>
  );
}
