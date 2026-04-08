import { redirect } from '@sveltejs/kit';

// fetch data from api/books/+server.ts route
export const actions = {
	default: async ({ request }) => {
		// [TODO] call backend (submit books, start recs job)
		const data = await request.formData();

		redirect(303, '/loading');
		return { success: true };
	}
};
